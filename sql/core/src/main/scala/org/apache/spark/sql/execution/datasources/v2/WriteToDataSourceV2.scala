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

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.continuous.{CommitPartitionEpoch, EpochCoordinatorRef}
import org.apache.spark.sql.sources.v2.writer._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * The logical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2(writer: DataSourceV2Writer, query: LogicalPlan) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil
}

/**
 * The physical plan for writing data into data source v2.
 */
case class WriteToDataSourceV2Exec(writer: DataSourceV2Writer, query: SparkPlan) extends SparkPlan {
  override def children: Seq[SparkPlan] = Seq(query)
  override def output: Seq[Attribute] = Nil

  override protected def doExecute(): RDD[InternalRow] = {
    val writeTask = writer match {
      case w: SupportsWriteInternalRow => w.createInternalRowWriterFactory()
      case _ => new InternalRowDataWriterFactory(writer.createWriterFactory(), query.schema)
    }

    val rdd = query.execute()
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)

    logInfo(s"Start processing data source writer: $writer. " +
      s"The input RDD has ${messages.length} partitions.")

    EpochCoordinatorRef.forDriver(writer.asInstanceOf[SupportsContinuousWrite], SparkEnv.get)

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          DataWritingSparkTask.run(writeTask, context, iter),
        rdd.partitions.indices,
        (index, message: WriterCommitMessage) => messages(index) = message
      )

      logInfo(s"Data source writer $writer is committing.")
      writer.commit(messages)
      logInfo(s"Data source writer $writer committed.")
    } catch {
      case cause: Throwable =>
        logError(s"Data source writer $writer is aborting.")
        try {
          writer.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source writer $writer failed to abort.")
            cause.addSuppressed(t)
            throw new SparkException("Writing job failed.", cause)
        }
        logError(s"Data source writer $writer aborted.")
        throw new SparkException("Writing job aborted.", cause)
    }

    sparkContext.emptyRDD
  }
}

object DataWritingSparkTask extends Logging {
  def run(
      writeTask: DataWriterFactory[InternalRow],
      context: TaskContext,
      iter: Iterator[InternalRow]): WriterCommitMessage = {
    val dataWriter = writeTask.createDataWriter(context.partitionId(), context.attemptNumber())

    var currentMsg: WriterCommitMessage = null
    var epoch = 0
    do {
      // write the data and commit this writer.
      currentMsg = Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        iter.foreach(dataWriter.write)
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        val msg = dataWriter.commit()
        logInfo(s"Writer for partition ${context.partitionId()} committed.")
        msg
      })(catchBlock = {
        // If there is an error, abort this writer
        logError(s"Writer for partition ${context.partitionId()} is aborting.")
        dataWriter.abort()
        logError(s"Writer for partition ${context.partitionId()} aborted.")
      })

      EpochCoordinatorRef.forExecutor(SparkEnv.get).send(
        CommitPartitionEpoch(context.partitionId(), epoch, currentMsg)
      )
      epoch += 1
    } while (true)

    currentMsg
  }
}

class InternalRowDataWriterFactory(
    rowWriterFactory: DataWriterFactory[Row],
    schema: StructType) extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[InternalRow] = {
    new InternalRowDataWriter(
      rowWriterFactory.createDataWriter(partitionId, attemptNumber),
      RowEncoder.apply(schema).resolveAndBind())
  }
}

class InternalRowDataWriter(rowWriter: DataWriter[Row], encoder: ExpressionEncoder[Row])
  extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = rowWriter.write(encoder.fromRow(record))

  override def commit(): WriterCommitMessage = rowWriter.commit()

  override def abort(): Unit = rowWriter.abort()
}
