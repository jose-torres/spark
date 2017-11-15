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

package org.apache.spark.sql.execution.datasources.v2

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.{MemoryWriterCommitMessage, ProcessingTimeExecutor, StreamExecution}
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.reader.{ContinuousDataReader, DataReader, ReadTask}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.SystemClock

// This can't be a structural type because we need to mutate its atomic vars.
class ContinuousDataSourceRDDIter(
    startEpoch: Long,
    epochEndpoint: RpcEndpointRef,
    context: TaskContext,
    reader: DataReader[UnsafeRow])
  extends Iterator[UnsafeRow] {
  private[this] var valuePrepared = false

  var currentEpoch = new AtomicLong(startEpoch)

  var outputMarker = new AtomicBoolean(false)

  override def hasNext: Boolean = {
    // If we're supposed to output a marker,
    //  * Report the current underlying row as the start of the next epoch.
    //  * Reset the outputMarker flag. (TODO this isn't the right place maybe)
    //  * Propagate hasNext = false (our marker boundary) to the writer.
    if (outputMarker.getAndSet(false)) {
      val offsetJson = reader match {
        case r: RowToUnsafeDataReader =>
          r.rowReader.asInstanceOf[ContinuousDataReader[Row]].getOffset().json
        case _ => throw new IllegalStateException("must have ContinuousDataReader[Row]")
      }
      epochEndpoint.send(ReportPartitionOffset(
        context.partitionId(), currentEpoch.get(), offsetJson))
      return false
    }

    wrappedHasNext
  }

  // Check whether the wrapped task has next.
  // We call this from next() to avoid doing the epoch check there.
  private def wrappedHasNext: Boolean = {
    if (!valuePrepared) {
      valuePrepared = reader.next()
    }
    valuePrepared
  }

  override def next(): UnsafeRow = {
    if (!wrappedHasNext) {
      throw new java.util.NoSuchElementException("End of stream")
    }
    valuePrepared = false
    reader.get()
  }
}

class ContinuousDataSourceRDD(
    sc: SparkContext,
    @transient private val readTasks: java.util.List[ReadTask[UnsafeRow]])
  extends RDD[UnsafeRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] = {
    readTasks.asScala.zipWithIndex.map {
      case (readTask, index) => new DataSourceRDDPartition(index, readTask)
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[UnsafeRow] = {
    val reader = split.asInstanceOf[DataSourceRDDPartition].readTask.createDataReader()
    context.addTaskCompletionListener(_ => reader.close())

    val epochEndpoint = EpochCoordinatorRef.forExecutor(
      context.getLocalProperty(StreamExecution.QUERY_ID_KEY), SparkEnv.get)
    val iter = new ContinuousDataSourceRDDIter(
      epochEndpoint.askSync[Long](GetCurrentEpoch()), epochEndpoint, context, reader)

    val epochPollThread = new Thread(new Runnable {
      override def run: Unit = {
        ProcessingTimeExecutor(ProcessingTime(900), new SystemClock())
          .execute { () =>
            if (context.isInterrupted()) {
              return
            } else {
              val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch())
              val currentEpoch = iter.currentEpoch.getAndSet(newEpoch)
              LocalCurrentEpochs.epoch = newEpoch
              if (currentEpoch != newEpoch) {
                val oldMarkerState = iter.outputMarker.getAndSet(true)
                logError(s"Set marker for epoch $newEpoch")
                // TODO this isn't synchronized right
                // we need to ensure this comes before next writer commit I think
                // we probably need to move the whole thing to writer
                /* if (oldMarkerState) {
                  val commit = CommitPartitionEpoch(
                    context.partitionId(),
                    currentEpoch,
                    MemoryWriterCommitMessage(context.partitionId(), Seq()))
                  epochEndpoint.send(commit)
                } */
              }

              // do next trigger
              true
            }
          }
      }
    })

    epochPollThread.setDaemon(true)
    epochPollThread.start()
    new InterruptibleIterator(context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].readTask.preferredLocations()
  }
}
