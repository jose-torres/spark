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
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.continuous._
import org.apache.spark.sql.sources.v2.reader.{ContinuousDataReader, DataReader, ReadTask}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.SystemClock

// This can't be a structural type because we need to mutate its atomic vars.
class ContinuousDataSourceRDDIter(
    epochEndpoint: RpcEndpointRef,
    context: TaskContext,
    reader: DataReader[UnsafeRow])
  extends Iterator[UnsafeRow] {

  private[this] var valuePrepared = false

  var currentEpoch = {
    val startEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong
    val offsetJson = reader match {
      case r: ContinuousDataReader[UnsafeRow] => r.getOffset().json
      case _ => throw new IllegalStateException("must have ContinuousDataReader[UnsafeRow]")
    }
    epochEndpoint.send(ReportPartitionOffset(
      context.partitionId(), startEpoch, offsetJson))

    startEpoch
  }

  override def hasNext: Boolean = {
    if (context.isInterrupted()) {
      // On interrupt, end the iterator so the writer can stop itself, but don't report a new epoch.
      return false
    }
    // When we see a marker, report the reader's current next offset as the start of the next epoch.
    val result = wrappedHasNext
    if (!result) {
      val offsetJson = reader match {
        case r: ContinuousDataReader[UnsafeRow] => r.getOffset().json
        case _ => throw new IllegalStateException("must have ContinuousDataReader[UnsafeRow]")
      }
      currentEpoch += 1
      epochEndpoint.send(ReportPartitionOffset(
        context.partitionId(), currentEpoch, offsetJson))
    }

    result
  }

  // Check whether the wrapped data reader has next.
  private def wrappedHasNext: Boolean = {
    if (!valuePrepared) {
      valuePrepared = reader.next()
    }
    valuePrepared
  }

  override def next(): UnsafeRow = {
    if (!valuePrepared) {
      throw new java.util.NoSuchElementException(
        "Illegal access pattern - called next without a preceding hasNext returning true")
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

    val epochEndpoint = EpochCoordinatorRef.get(
      context.getLocalProperty(StreamExecution.QUERY_ID_KEY), SparkEnv.get)
    val iter = new ContinuousDataSourceRDDIter(epochEndpoint, context, reader)

    val epochPollThread = new Thread(new Runnable {
      var offset: Offset = _
      var currentEpoch = context.getLocalProperty(ContinuousExecution.START_EPOCH_KEY).toLong
      override def run: Unit = {
        ProcessingTimeExecutor(ProcessingTime(900), new SystemClock())
          .execute { () =>
            if (context.isInterrupted()) {
              // Call outputMarker so the reader knows to interrupt any long polls.
              reader.asInstanceOf[ContinuousDataReader[UnsafeRow]].outputMarker()
              false
            } else {
              val newEpoch = epochEndpoint.askSync[Long](GetCurrentEpoch())
              if (currentEpoch < newEpoch) {
                for (_ <- currentEpoch to newEpoch - 1) {
                  reader.asInstanceOf[ContinuousDataReader[UnsafeRow]].outputMarker()
                }
                logDebug(s"Set marker for epoch $newEpoch")
                currentEpoch = newEpoch
              }

              true
            }
          }
      }
    })

    epochPollThread.setDaemon(true)
    epochPollThread.start()
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataSourceRDDPartition].readTask.preferredLocations()
  }
}
