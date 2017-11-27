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

import scala.collection.JavaConverters._

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.streaming.{LongOffset, Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceV2, DataSourceV2Options}
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{LongType, StructField, StructType, TimestampType}

object ContinuousRateStreamSource {
  val NUM_PARTITIONS = "numPartitions"
  val ROWS_PER_SECOND = "rowsPerSecond"
}

case class ContinuousRateStreamOffset(partitionToStartValue: Map[Int, Long]) extends Offset {
  implicit val defaultFormats: DefaultFormats = DefaultFormats
  override val json = Serialization.write(partitionToStartValue)
}

class ContinuousRateStreamReader(options: DataSourceV2Options)
  extends DataSourceV2Reader with ContinuousReader {
  implicit val defaultFormats: DefaultFormats = DefaultFormats

  val numPartitions = options.get(ContinuousRateStreamSource.NUM_PARTITIONS).orElse("5").toInt
  val rowsPerSecond = options.get(ContinuousRateStreamSource.ROWS_PER_SECOND).orElse("6").toLong

  override def mergeOffsets(offsets: Array[Offset]): Offset = {

    assert(offsets.length == numPartitions)
    val newMap = offsets.map {
      case o: SerializedOffset =>
        ContinuousRateStreamOffset(Serialization.read[Map[Int, Long]](o.json))
      case o: ContinuousRateStreamOffset => o
      case _ => null
    }.map(_.partitionToStartValue)
      .reduce((a, b) => a ++ b)
    ContinuousRateStreamOffset(newMap)
  }

  override def readSchema(): StructType = {
    StructType(
        StructField("timestamp", TimestampType, false) ::
        StructField("value", LongType, false) :: Nil)
  }

  override def createReadTasks(
      offset: java.util.Optional[Offset]): java.util.List[ReadTask[Row]] = {
    val partitionStartMap = Option(offset.orElse(null)).map {
      case o: ContinuousRateStreamOffset => o.partitionToStartValue
      case s: SerializedOffset => Serialization.read[Map[Int, Long]](s.json)
      case _ => throw new IllegalArgumentException("invalid offset type for ContinuousRateSource")
    }
    if (partitionStartMap.exists(_.keySet.size > numPartitions)) {
      throw new IllegalArgumentException("Start offset contained too many partitions.")
    }
    val perPartitionRate = rowsPerSecond.toDouble / numPartitions.toDouble

    Range(0, numPartitions).map { n =>
      // If the offset doesn't have a value for this partition, start from the beginning.
      val start = partitionStartMap.flatMap(_.get(n)).getOrElse(0L + n)
      // Have each partition advance by numPartitions each row, with starting points staggered
      // by their partition index.
      RateStreamReadTask(start, n, numPartitions, perPartitionRate).asInstanceOf[ReadTask[Row]]
    }.asJava
  }

  // TODO move
  override def commit(end: Offset): Unit = {}
  override def stop(): Unit = {}

}

case class RateStreamReadTask(
    startValue: Long, partitionIndex: Int, increment: Long, rowsPerSecond: Double)
  extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] =
    new RateStreamDataReader(startValue, partitionIndex, increment, rowsPerSecond.toLong)
}

class RateStreamDataReader(
    startValue: Long, partitionIndex: Int, increment: Long, rowsPerSecond: Long)
  extends ContinuousDataReader[Row] {

  private var nextReadTime = 0L
  private var numReadRows = 0L

  private var currentValue = startValue
  private var currentRow: Row = null

  private var numQueuedMarkers = 0

  override def next(): Boolean = {
    // Set the timestamp for the first time.
    if (currentRow == null) nextReadTime = System.currentTimeMillis() + 1000

    if (numQueuedMarkers > 0) synchronized {
      numQueuedMarkers -= 1
      return false
    }

    if (numReadRows == rowsPerSecond) {
      // Sleep until we reach the next second.
      while (System.currentTimeMillis < nextReadTime) {
        Thread.sleep(nextReadTime - System.currentTimeMillis)
      }
      numReadRows = 0
      nextReadTime += 1000
    }

    currentRow = Row(
      DateTimeUtils.toJavaTimestamp(DateTimeUtils.fromMillis(System.currentTimeMillis)),
      currentValue)
    currentValue += increment
    numReadRows += 1

    true
  }

  override def get: Row = currentRow

  override def close(): Unit = {}

  override def outputMarker(): Unit = synchronized {
    numQueuedMarkers += 1
  }

  // We use the value corresponding to partition 0 as the offset.
  override def getOffset(): Offset = ContinuousRateStreamOffset(Map(partitionIndex -> currentValue))
}
