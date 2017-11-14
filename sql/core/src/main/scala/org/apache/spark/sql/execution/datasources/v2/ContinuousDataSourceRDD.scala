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

import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.execution.streaming.continuous.{CommitPartitionEpoch, EpochCoordinatorRef, GetCurrentEpoch, ReportPartitionOffset}
import org.apache.spark.sql.sources.v2.reader.{ContinuousDataReader, ReadTask}

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
    val iter = new Iterator[UnsafeRow] {
      val epochEndpoint = EpochCoordinatorRef.forExecutor(
        context.getLocalProperty(StreamExecution.QUERY_ID_KEY), SparkEnv.get)
      private[this] var valuePrepared = false

      private[this] var currentEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch())

      override def hasNext: Boolean = {
        val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch())
        if (currentEpoch == newEpoch) {
          wrappedHasNext
        } else {
          currentEpoch = newEpoch
          val offsetJson = reader match {
            case r: RowToUnsafeDataReader =>
              r.rowReader.asInstanceOf[ContinuousDataReader[Row]].getOffset().json
            case _ => throw new IllegalStateException("must have ContinuousDataReader[Row]")
          }
          epochEndpoint.send(ReportPartitionOffset(context.partitionId(), newEpoch, offsetJson))
          false
        }
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
    new InterruptibleIterator(context, iter)
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].readTask.preferredLocations()
  }
}
