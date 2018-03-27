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

package org.apache.spark.sql.execution.streaming.continuous

import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{DataWritingSparkTask, InternalRowDataWriterFactory}
import org.apache.spark.sql.execution.datasources.v2.DataWritingSparkTask.{logError, logInfo}
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.util.Utils

/**
 * The physical plan for writing data into a continuous processing [[StreamWriter]].
 */
case class ContinuousWriteExec(writer: StreamWriter, query: SparkPlan)
    extends SparkPlan with Logging {
  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val writerFactory = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new InternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val rdd = query.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")
    // Let the epoch coordinator know how many partitions the write RDD has.
    EpochCoordinatorRef.get(
        sparkContext.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
        sparkContext.env)
      .askSync[Unit](SetWriterPartitions(rdd.getNumPartitions))

    try {
      // Force the RDD to run so continuous processing starts; no data is actually being collected
      // to the driver, as ContinuousWriteRDD outputs nothing.
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          ContinuousWriteExec.run(writerFactory, context, iter),
        rdd.partitions.indices)
    } catch {
      case _: InterruptedException =>
        // Interruption is how continuous queries are ended, so accept and ignore the exception.
      case cause: Throwable =>
        cause match {
          // Do not wrap interruption exceptions that will be handled by streaming specially.
          case _ if StreamExecution.isInterruptionException(cause) => throw cause
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw new SparkException("Writing job aborted.", e)
          case _ => throw cause
        }
    }

    sparkContext.emptyRDD
  }
}

object ContinuousWriteExec extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): Unit = {
    val epochCoordinator = EpochCoordinatorRef.get(
      context.getLocalProperty(ContinuousExecution.EPOCH_COORDINATOR_ID_KEY),
      SparkEnv.get)
    val currentMsg: WriterCommitMessage = null
    var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong

    do {
      var dataWriter: DataWriter[InternalRow] = null
      // write the data and commit this writer.
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        try {
          dataWriter = writeTask.createDataWriter(
            context.partitionId(), context.attemptNumber(), currentEpoch)
          while (iter.hasNext) {
            dataWriter.write(iter.next())
          }
          logInfo(s"Writer for partition ${context.partitionId()} is committing.")
          val msg = dataWriter.commit()
          logInfo(s"Writer for partition ${context.partitionId()} committed.")
          epochCoordinator.send(
            CommitPartitionEpoch(context.partitionId(), currentEpoch, msg)
          )
          currentEpoch += 1
        } catch {
          case _: InterruptedException =>
          // Continuous shutdown always involves an interrupt. Just finish the task.
        }
      })(catchBlock = {
        // If there is an error, abort this writer. We enter this callback in the middle of
        // rethrowing an exception, so runContinuous will stop executing at this point.
        logError(s"Writer for partition ${context.partitionId()} is aborting.")
        if (dataWriter != null) dataWriter.abort()
        logError(s"Writer for partition ${context.partitionId()} aborted.")
      })
    } while (!context.isInterrupted())
  }
}
