/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.streaming

import java.io.{InterruptedIOException, IOException, UncheckedIOException}
import java.nio.channels.ClosedByInterruptException
import java.util.UUID
import java.util.concurrent.{CountDownLatch, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, CurrentBatchTimestamp, CurrentDate, CurrentTimestamp}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.command.StreamingExplainCommand
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, WriteToDataSourceV2}
import org.apache.spark.sql.execution.streaming.continuous.{EpochCoordinatorRef, IncrementAndGetEpoch}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader.{ContinuousReader, DataSourceV2Reader}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{Clock, UninterruptibleThread, Utils}

/**
 * Manages the execution of a streaming Spark SQL query that is occurring in a separate thread.
 * Unlike a standard query, a streaming query executes repeatedly each time new data arrives at any
 * [[Source]] present in the query plan. Whenever new data arrives, a [[QueryExecution]] is created
 * and the results are committed transactionally to the given [[Sink]].
 *
 * @param deleteCheckpointOnStop whether to delete the checkpoint if the query is stopped without
 *                               errors
 */
class ContinuousExecution(
    override val sparkSession: SparkSession,
    override val name: String,
    private val checkpointRoot: String,
    analyzedPlan: LogicalPlan,
    extraOptions: Map[String, String],
    val sink: ContinuousWriteSupport,
    val trigger: Trigger,
    val triggerClock: Clock,
    val outputMode: OutputMode,
    deleteCheckpointOnStop: Boolean)
  extends StreamingQuery with ProgressReporter with Logging {

  import org.apache.spark.sql.streaming.StreamingQueryListener._

  private val pollingDelayMs = sparkSession.sessionState.conf.streamingPollingDelay

  val resolvedCheckpointRoot = {
    val checkpointPath = new Path(checkpointRoot)
    val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
    checkpointPath.makeQualified(fs.getUri(), fs.getWorkingDirectory()).toUri.toString
  }

  /**
   * Tracks how much data we have processed and committed to the sink or state store from each
   * input source. Only the scheduler thread should read from or write to this field.
   */
  @volatile
  var committedOffsets = new StreamProgress

  /** The current epoch ID or -1 if execution has not yet been initialized. */
  protected var currentEpochId: Long = -1
  // TODO for interface
  protected def currentBatchId = currentEpochId
  protected def offsetSeqMetadata = OffsetSeqMetadata(
    batchWatermarkMs = 0, batchTimestampMs = 0, sparkSession.conf)
  protected def newData = Map()
  protected def availableOffsets = null
  override def processAllAvailable(): Unit = {}

  /** Metadata associated with the whole query */
  protected val streamMetadata: StreamMetadata = {
    val metadataPath = new Path(checkpointFile("metadata"))
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    StreamMetadata.read(metadataPath, hadoopConf).getOrElse {
      val newMetadata = new StreamMetadata(UUID.randomUUID.toString)
      StreamMetadata.write(newMetadata, metadataPath, hadoopConf)
      newMetadata
    }
  }

  override val id: UUID = UUID.fromString(streamMetadata.id)

  override val runId: UUID = UUID.randomUUID

  /**
   * Pretty identified string of printing in logs. Format is
   * If name is set "queryName [id = xyz, runId = abc]" else "[id = xyz, runId = abc]"
   */
  private val prettyIdString =
    Option(name).map(_ + " ").getOrElse("") + s"[id = $id, runId = $runId]"

  /**
   * All stream sources present in the query plan. This will be set when generating logical plan.
   */
  @volatile protected var sources: Seq[ContinuousReadSupport] = Seq.empty

  /**
   * A list of unique sources in the query plan. This will be set when generating logical plan.
   */
  @volatile private var uniqueSources: Seq[ContinuousReadSupport] = Seq.empty

  override lazy val logicalPlan: LogicalPlan = {
    assert(streamExecutionThread eq Thread.currentThread,
      "logicalPlan must be initialized in StreamExecutionThread " +
        s"but the current thread was ${Thread.currentThread}")
    var nextSourceId = 0L
    val toExecutionRelationMap = MutableMap[ContinuousRelation, ContinuousExecutionRelation]()
    val _logicalPlan = analyzedPlan.transform {
      case r @ ContinuousRelation(dataSource, _, extraReaderOptions, output) =>
        toExecutionRelationMap.getOrElseUpdate(r, {
          // Materialize reader to avoid creating it in every batch
          // TODO: actually materialize it
          val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
          val source = dataSource
          nextSourceId += 1
          // We still need to use the previous `output` instead of `source.schema` as attributes in
          // "df.logicalPlan" has already used attributes of the previous `output`.
          ContinuousExecutionRelation(source, extraReaderOptions, output)(sparkSession)
        })
    }
    sources = _logicalPlan.collect {
      case s: ContinuousExecutionRelation => s.source
    }
    uniqueSources = sources.distinct
    _logicalPlan
  }

  private val triggerExecutor = trigger match {
    case _ => ProcessingTimeExecutor(ProcessingTime(999), triggerClock)
    // TODO input
    /* case t: ProcessingTime => ProcessingTimeExecutor(t, triggerClock)
    case OneTimeTrigger => OneTimeExecutor()
    case _ => throw new IllegalStateException(s"Unknown type of trigger: $trigger") */
  }

  /** Defines the internal state of execution */
  private val state = new AtomicReference[State](INITIALIZING)

  @volatile
  var lastExecution: IncrementalExecution = _

  @volatile
  private var streamDeathCause: StreamingQueryException = null

  /* Get the call site in the caller thread; will pass this into the micro batch thread */
  private val callSite = Utils.getCallSite()

  /** Used to report metrics to coda-hale. This uses id for easier tracking across restarts. */
  lazy val streamMetrics = new MetricsReporter(
    this, s"spark.streaming.${Option(name).getOrElse(id)}")

  /**
   * The thread that runs the micro-batches of this stream. Note that this thread must be
   * [[org.apache.spark.util.UninterruptibleThread]] to workaround KAFKA-1894: interrupting a
   * running `KafkaConsumer` may cause endless loop.
   */
  lazy val streamExecutionThread = {
    new StreamExecutionThread(s"stream execution thread for $prettyIdString") {
      override def run(): Unit = {
        // To fix call site like "run at <unknown>:0", we bridge the call site from the caller
        // thread to this micro batch thread
        sparkSession.sparkContext.setCallSite(callSite)
        runContinuous()
      }
    }
  }

  private val initializationLatch = new CountDownLatch(1)
  private val startLatch = new CountDownLatch(1)
  private val terminationLatch = new CountDownLatch(1)

  /**
   * A write-ahead-log that records the offsets that are present in each epoch. To ensure integrity
   * of epochs across partition failures, we must write all partitions to this log for epoch N + 1
   * before epoch N is committed.
   */
  val offsetLog = new OffsetSeqLog(sparkSession, checkpointFile("offsets"))

  /**
   * A log that records the epochs that have durably completed.
   */
  val epochCommitLog = new BatchCommitLog(sparkSession, checkpointFile("commits"))

  /** Whether all fields of the query have been initialized */
  private def isInitialized: Boolean = state.get != INITIALIZING

  /** Whether the query is currently active or not */
  override def isActive: Boolean = state.get != TERMINATED

  /** Returns the [[StreamingQueryException]] if the query was terminated by an exception. */
  override def exception: Option[StreamingQueryException] = Option(streamDeathCause)

  /** Returns the path of a file with `name` in the checkpoint directory. */
  private def checkpointFile(name: String): String =
    new Path(new Path(resolvedCheckpointRoot), name).toUri.toString

  /**
   * Starts the execution. This returns only after the thread has started and [[QueryStartedEvent]]
   * has been posted to all the listeners.
   */
  def start(): Unit = {
    logInfo(s"Starting $prettyIdString. Use $resolvedCheckpointRoot to store the query checkpoint.")
    streamExecutionThread.setDaemon(true)
    streamExecutionThread.start()
    startLatch.await()  // Wait until thread started and QueryStart event has been posted
  }

  /**
   * Repeatedly attempts to run batches as data arrives.
   *
   * Note that this method ensures that [[QueryStartedEvent]] and [[QueryTerminatedEvent]] are
   * posted such that listeners are guaranteed to get a start event before a termination.
   * Furthermore, this method also ensures that [[QueryStartedEvent]] event is posted before the
   * `start()` method returns.
   */
  private def runContinuous(): Unit = {
    try {
      sparkSession.sparkContext.setJobGroup(runId.toString, getDescriptionString,
        interruptOnCancel = true)
      sparkSession.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, id.toString)
      if (sparkSession.sessionState.conf.streamingMetricsEnabled) {
        sparkSession.sparkContext.env.metricsSystem.registerSource(streamMetrics)
      }

      // `postEvent` does not throw non fatal exception.
      postEvent(new QueryStartedEvent(id, runId, name))

      // Unblock starting thread
      startLatch.countDown()

      // While active, repeatedly attempt to run batches.
      SparkSession.setActiveSession(sparkSession)

      updateStatusMessage("Initializing sources")
      // force initialization of the logical plan so that the sources can be created
      logicalPlan

      // Isolated spark session to run the continuous query with.
      val sparkSessionToRunBatches = sparkSession.cloneSession()
      // Adaptive execution can change num shuffle partitions, disallow
      sparkSessionToRunBatches.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      // Disable cost-based join optimization as we do not want stateful operations to be rearranged
      sparkSessionToRunBatches.conf.set(SQLConf.CBO_ENABLED.key, "false")

      if (state.compareAndSet(INITIALIZING, ACTIVE)) {
        // Unblock `awaitInitialization`
        initializationLatch.countDown()

        val startOffsets = getStartOffsets(sparkSessionToRunBatches)
        sparkSession.sparkContext.setJobDescription(getDescriptionString)
        logDebug(s"Stream running from $committedOffsets")
        runFromOffsets(startOffsets, sparkSessionToRunBatches)
        updateStatusMessage("Stopped")
      } else {
        // `stop()` is already called. Let `finally` finish the cleanup.
      }
    } catch {
      case e if isInterruptedByStop(e) =>
        // interrupted by stop()
        updateStatusMessage("Stopped")
      case e: IOException if e.getMessage != null
        && e.getMessage.startsWith(classOf[InterruptedException].getName)
        && state.get == TERMINATED =>
        // This is a workaround for HADOOP-12074: `Shell.runCommand` converts `InterruptedException`
        // to `new IOException(ie.toString())` before Hadoop 2.8.
        updateStatusMessage("Stopped")
      case e: Throwable =>
        streamDeathCause = new StreamingQueryException(
          toDebugString(includeLogicalPlan = isInitialized),
          s"Query $prettyIdString terminated with exception: ${e.getMessage}",
          e,
          committedOffsets.toOffsetSeq(sources, offsetSeqMetadata).toString,
          "fake-available-offset-str")
        logError(s"Query $prettyIdString terminated with error", e)
        updateStatusMessage(s"Terminated with exception: ${e.getMessage}")
        // Rethrow the fatal errors to allow the user using `Thread.UncaughtExceptionHandler` to
        // handle them
        if (!NonFatal(e)) {
          throw e
        }
    } finally streamExecutionThread.runUninterruptibly {
      // The whole `finally` block must run inside `runUninterruptibly` to avoid being interrupted
      // when a query is stopped by the user. We need to make sure the following codes finish
      // otherwise it may throw `InterruptedException` to `UncaughtExceptionHandler` (SPARK-21248).

      // Release latches to unblock the user codes since exception can happen in any place and we
      // may not get a chance to release them
      startLatch.countDown()
      initializationLatch.countDown()

      try {
        stopSources()
        state.set(TERMINATED)
        currentStatus = status.copy(isTriggerActive = false, isDataAvailable = false)

        // Update metrics and status
        sparkSession.sparkContext.env.metricsSystem.removeSource(streamMetrics)

        // Notify others
        sparkSession.streams.notifyQueryTermination(ContinuousExecution.this)
        postEvent(
          new QueryTerminatedEvent(id, runId, exception.map(_.cause).map(Utils.exceptionString)))

        // Delete the temp checkpoint only when the query didn't fail
        if (deleteCheckpointOnStop && exception.isEmpty) {
          val checkpointPath = new Path(resolvedCheckpointRoot)
          try {
            val fs = checkpointPath.getFileSystem(sparkSession.sessionState.newHadoopConf())
            fs.delete(checkpointPath, true)
          } catch {
            case NonFatal(e) =>
              // Deleting temp checkpoint folder is best effort, don't throw non fatal exceptions
              // when we cannot delete them.
              logWarning(s"Cannot delete $checkpointPath", e)
          }
        }
      } finally {
        terminationLatch.countDown()
      }
    }
  }

  private def isInterruptedByStop(e: Throwable): Boolean = {
    if (state.get == TERMINATED) {
      e match {
        // InterruptedIOException - thrown when an I/O operation is interrupted
        // ClosedByInterruptException - thrown when an I/O operation upon a channel is interrupted
        case _: InterruptedException | _: InterruptedIOException | _: ClosedByInterruptException =>
          true
        // The cause of the following exceptions may be one of the above exceptions:
        //
        // UncheckedIOException - thrown by codes that cannot throw a checked IOException, such as
        //                        BiFunction.apply
        // ExecutionException - thrown by codes running in a thread pool and these codes throw an
        //                      exception
        // UncheckedExecutionException - thrown by codes that cannot throw a checked
        //                               ExecutionException, such as BiFunction.apply
        case e2 @ (_: UncheckedIOException | _: ExecutionException | _: UncheckedExecutionException)
          if e2.getCause != null =>
          isInterruptedByStop(e2.getCause)
        case _ =>
          false
      }
    } else {
      false
    }
  }

  /**
   * Populate the start offsets to start the execution at the current offsets stored in the sink
   * (i.e. avoid reprocessing data that we have already processed). This function must be called
   * before any processing occurs and will populate the following fields:
   *  - currentBatchId
   *  - committedOffsets
   *  - availableOffsets
   *  The basic structure of this method is as follows:
   *
   *  Identify (from the offset log) the offsets used to run the last batch
   *  IF last batch exists THEN
   *    Set the next batch to be executed as the last recovered batch
   *    Check the commit log to see which batch was committed last
   *    IF the last batch was committed THEN
   *      Call getBatch using the last batch start and end offsets
   *      // ^^^^ above line is needed since some sources assume last batch always re-executes
   *      Setup for a new batch i.e., start = last batch end, and identify new end
   *    DONE
   *  ELSE
   *    Identify a brand new batch
   *  DONE
   */
  private def getStartOffsets(sparkSessionToRunBatches: SparkSession): OffsetSeq = {
    offsetLog.getLatest() match {
      case Some((latestEpochId, nextOffsets)) =>
        currentEpochId = latestEpochId
        // Initialize committed offsets to the second latest batch id in the offset log, which
        // is the latest committed epoch.
        // TODO this sequencing isn't right
        // TODO we can't allow epoch to advance until we got offsets for the previous one
        if (latestEpochId != 0) {
          val secondLatestEpochId = offsetLog.get(latestEpochId - 1).getOrElse {
            throw new IllegalStateException(s"batch ${latestEpochId - 1} doesn't exist")
          }
          committedOffsets = secondLatestEpochId.toStreamProgress(sources)
        }

        logDebug(s"Resuming at epoch $currentEpochId with committed offsets " +
          s"$committedOffsets and available offsets $availableOffsets")
        nextOffsets
      case None =>
        // We are starting this stream for the first time. Offsets are all None.
        logInfo(s"Starting new streaming query.")
        currentEpochId = 0
        OffsetSeq.fill(sources.map(_ => null): _*)
    }
  }

  /**
   * Processes any data available between `availableOffsets` and `committedOffsets`.
   * @param sparkSessionToRunBatch Isolated [[SparkSession]] to run this batch with.
   */
  private def runFromOffsets(offsets: OffsetSeq, sparkSessionToRunBatch: SparkSession): Unit = {
    import scala.collection.JavaConverters._
    // A list of attributes that will need to be updated.
    val replacements = new ArrayBuffer[(Attribute, Attribute)]
    // Replace sources in the logical plan with data that has arrived since the last batch.
    val withNewSources = logicalPlan transform {
      case ContinuousExecutionRelation(source, extraReaderOptions, output) =>
        // TODO multiple sources maybe?
        val nextSourceId = 0
        val metadataPath = s"$resolvedCheckpointRoot/sources/$nextSourceId"
        val reader = source.createContinuousReader(
          java.util.Optional.ofNullable(offsets.offsets(0).orNull),
          java.util.Optional.empty[StructType](),
          metadataPath,
          new DataSourceV2Options(extraReaderOptions.asJava))
        val newOutput = reader.readSchema().toAttributes

        assert(output.size == newOutput.size,
          s"Invalid reader: ${Utils.truncatedString(output, ",")} != " +
            s"${Utils.truncatedString(newOutput, ",")}")
        replacements ++= output.zip(newOutput)
        DataSourceV2Relation(newOutput, reader)
    }

    // Rewire the plan to use the new attributes that were returned by the source.
    val replacementMap = AttributeMap(replacements)
    val triggerLogicalPlan = withNewSources transformAllExpressions {
      case a: Attribute if replacementMap.contains(a) =>
        replacementMap(a).withMetadata(a.metadata)
      // TODO properly handle timestamp
      case ct: CurrentTimestamp =>
        CurrentBatchTimestamp(0, ct.dataType)
      case cd: CurrentDate =>
        CurrentBatchTimestamp(0, cd.dataType, cd.timeZoneId)
    }

    val writer = sink.createContinuousWriter(
      s"$id",
      currentBatchId,
      triggerLogicalPlan.schema,
      outputMode,
      new DataSourceV2Options(extraOptions.asJava))
    val withSink = WriteToDataSourceV2(writer.get(), triggerLogicalPlan)

    val reader = withSink.collect {
      case r: DataSourceV2Relation => r.reader.asInstanceOf[ContinuousReader]
    }.head

    reportTimeTaken("queryPlanning") {
      lastExecution = new IncrementalExecution(
        sparkSessionToRunBatch,
        withSink,
        outputMode,
        checkpointFile("state"),
        runId,
        currentBatchId,
        offsetSeqMetadata)
      lastExecution.executedPlan // Force the lazy generation of execution plan
    }

    sparkSession.sparkContext.setLocalProperty(
      ContinuousExecution.START_EPOCH_KEY, currentEpochId.toString)

    // Use the parent Spark session since it's where this query is registered.
    // TODO: we should use runId for the endpoint to be safe against cross-contamination
    // from failed runs
    val epochEndpoint =
      EpochCoordinatorRef.create(
        writer.get(), reader, currentEpochId, id.toString, sparkSession, SparkEnv.get)
    val epochUpdateThread = new Thread(new Runnable {
      override def run: Unit = {
        triggerExecutor.execute(() => {
          startTrigger()

          if (isActive) {
            currentEpochId = epochEndpoint.askSync[Long](IncrementAndGetEpoch())
            logInfo(s"New epoch $currentEpochId is starting.")
          }

          isActive
        })
      }
    })

    try {
      epochUpdateThread.setDaemon(true)
      epochUpdateThread.start()

      reportTimeTaken("runContinuous") {
        SQLExecution.withNewExecutionId(sparkSessionToRunBatch, lastExecution)(lastExecution.toRdd)
      }
    } finally {
      SparkEnv.get.rpcEnv.stop(epochEndpoint)

      epochUpdateThread.interrupt()
      epochUpdateThread.join()
    }
  }

  override protected def postEvent(event: StreamingQueryListener.Event): Unit = {
    sparkSession.streams.postListenerEvent(event)
  }

  /** Stops all streaming sources safely. */
  private def stopSources(): Unit = {
    uniqueSources.foreach { source =>
      try {
        source.stop()
      } catch {
        case NonFatal(e) =>
          logWarning(s"Failed to stop streaming source: $source. Resources may have leaked.", e)
      }
    }
  }

  def addOffset(
      epoch: Long, reader: ContinuousReader, partitionOffsets: Seq[SerializedOffset]): Unit = {
    assert(sources.length == 1, "only one continuous source supported currently")

    val globalOffset = reader.mergeOffsets(partitionOffsets.toArray)
    synchronized {
      offsetLog.add(epoch, OffsetSeq.fill(globalOffset))
    }
  }

  def commit(epoch: Long): Unit = {
    // TODO: actually make the exceptions stop
    try {
      assert(sources.length == 1, "only one continuous source supported currently")
      synchronized {
        val offset = offsetLog.get(epoch + 1).get.offsets(0).get
        committedOffsets ++= Seq(sources(0) -> offset)
      }
    } catch {
      case _: Throwable =>
    }

    // TODO what happens if writer commit worked but this one failed? I think that the idempotency
    // means we can run a batch between two offsets butn eed to check
  }

  /**
   * Signals to the execution thread that it should stop running after the next
   * epoch. This method blocks until the thread stops running.
   *
   * TODO does this work for continuous?
   */
  override def stop(): Unit = {
    // Set the state to TERMINATED so that the batching thread knows that it was interrupted
    // intentionally
    state.set(TERMINATED)
    if (streamExecutionThread.isAlive) {
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
      streamExecutionThread.interrupt()
      streamExecutionThread.join()
      // microBatchThread may spawn new jobs, so we need to cancel again to prevent a leak
      sparkSession.sparkContext.cancelJobGroup(runId.toString)
    }
    logError(s"Query $prettyIdString was stopped")
  }

  /** A flag to indicate that a batch has completed with no new data available. */
  @volatile private var noNewData = false

  /**
   * Assert that the await APIs should not be called in the stream thread. Otherwise, it may cause
   * dead-lock, e.g., calling any await APIs in `StreamingQueryListener.onQueryStarted` will block
   * the stream thread forever.
   */
  private def assertAwaitThread(): Unit = {
    if (streamExecutionThread eq Thread.currentThread) {
      throw new IllegalStateException(
        "Cannot wait for a query state from the same thread that is running the query")
    }
  }

  /**
   * Await until all fields of the query have been initialized.
   */
  def awaitInitialization(timeoutMs: Long): Unit = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
    initializationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(): Unit = {
    assertAwaitThread()
    terminationLatch.await()
    if (streamDeathCause != null) {
      throw streamDeathCause
    }
  }

  override def awaitTermination(timeoutMs: Long): Boolean = {
    assertAwaitThread()
    require(timeoutMs > 0, "Timeout has to be positive")
    terminationLatch.await(timeoutMs, TimeUnit.MILLISECONDS)
    if (streamDeathCause != null) {
      throw streamDeathCause
    } else {
      !isActive
    }
  }

  /** Expose for tests */
  def explainInternal(extended: Boolean): String = {
    if (lastExecution == null) {
      "No physical plan. Waiting for data."
    } else {
      val explain = StreamingExplainCommand(lastExecution, extended = extended)
      sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect()
        .map(_.getString(0)).mkString("\n")
    }
  }

  override def explain(extended: Boolean): Unit = {
    // scalastyle:off println
    println(explainInternal(extended))
    // scalastyle:on println
  }

  override def explain(): Unit = explain(extended = false)

  override def toString: String = {
    s"Streaming Query $prettyIdString [state = $state]"
  }

  private def toDebugString(includeLogicalPlan: Boolean): String = {
    val debugString =
      s"""|=== Streaming Query ===
          |Identifier: $prettyIdString
          |Current Committed Offsets: $committedOffsets
          |Current Available Offsets: $availableOffsets
          |
          |Current State: $state
          |Thread State: ${streamExecutionThread.getState}""".stripMargin
    if (includeLogicalPlan) {
      debugString + s"\n\nLogical Plan:\n$logicalPlan"
    } else {
      debugString
    }
  }

  private def getDescriptionString: String = {
    val batchDescription = if (currentBatchId < 0) "init" else currentBatchId.toString
    Option(name).map(_ + "<br/>").getOrElse("") +
      s"id = $id<br/>runId = $runId<br/>batch = $batchDescription"
  }
}

object ContinuousExecution {
  val START_EPOCH_KEY = "__continuous_start_epoch"
}
