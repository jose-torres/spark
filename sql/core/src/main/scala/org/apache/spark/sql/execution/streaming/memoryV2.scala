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

import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LocalRelation, Statistics}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.sources.v2.{DataSourceV2, DataSourceV2Options, ReadMicroBatchSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataSourceV2Reader, ReadTask}
import org.apache.spark.sql.sources.v2.writer.{DataSourceV2Writer, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils


case class MemoryStreamV2[A : Encoder](implicit sqlContext: SQLContext)
  extends DataSourceV2 with ReadMicroBatchSupport with Logging {
  protected val encoder = encoderFor[A]
  protected val logicalPlan =
    StreamingExecutionRelationV2(this, encoder.schema.toAttributes)(sqlContext.sparkSession)
  protected val output = logicalPlan.output

  /**
   * All batches from `lastCommittedOffset + 1` to `currentOffset`, inclusive.
   * Stored in a ListBuffer to facilitate removing committed batches.
   */
  @GuardedBy("this")
  protected val batches = new ListBuffer[Seq[A]]

  @GuardedBy("this")
  protected var currentStart: LongOffset = new LongOffset(-1)
  @GuardedBy("this")
  protected var currentEnd: LongOffset = new LongOffset(-1)
  @GuardedBy("this")
  protected var currentOffset: LongOffset = new LongOffset(-1)
  @GuardedBy("this")
  protected var lastOffsetCommitted: LongOffset = new LongOffset(-1)

  override def setOffsetRange(start: java.util.Optional[Offset], end: Offset): Unit = {
    val scalaStart = Option[Offset](start.orElse(null))
    currentStart = scalaStart.flatMap(off => LongOffset.convert(off)).getOrElse(new LongOffset(-1))
    currentEnd = LongOffset.convert(end).getOrElse(new LongOffset(-1))
  }

  override def getOffset(): java.util.Optional[Offset] = synchronized {
    if (currentOffset.offset == -1) {
      java.util.Optional.ofNullable(null)
    } else {
      java.util.Optional.of(currentOffset)
    }
  }

  def toDS(): Dataset[A] = {
    Dataset(sqlContext.sparkSession, logicalPlan)
  }

  def toDF(): DataFrame = {
    Dataset.ofRows(sqlContext.sparkSession, logicalPlan)
  }

  def addData(data: A*): Offset = {
    addData(data.toTraversable)
  }

  def addData(data: TraversableOnce[A]): Offset = {
    val dataSeq = data.toSeq
    logDebug(s"Adding data: $dataSeq")
    this.synchronized {
      currentOffset = currentOffset + 1
      batches += dataSeq
      currentOffset
    }
  }

  override def toString: String = s"MemoryStream[${Utils.truncatedString(output, ",")}]"

  override def createReader(options: DataSourceV2Options): MemoryStreamV2Reader[A] = {
    val startOrdinal =
      LongOffset.convert(currentStart).getOrElse(LongOffset(-1)).offset.toInt + 1
    val endOrdinal = LongOffset.convert(currentEnd).getOrElse(LongOffset(-1)).offset.toInt + 1

    // Internal buffer only holds the batches after lastCommittedOffset.
    val newBlocks = synchronized {
      val sliceStart = startOrdinal - lastOffsetCommitted.offset.toInt - 1
      val sliceEnd = endOrdinal - lastOffsetCommitted.offset.toInt - 1
      batches.slice(sliceStart, sliceEnd)
    }

    logDebug(s"MemoryBatch [$startOrdinal, $endOrdinal]: ${newBlocks.flatten.mkString(", ")}")
    new MemoryStreamV2Reader(encoder.schema, newBlocks)
  }

  override def commit(end: Offset): Unit = synchronized {
    def check(newOffset: LongOffset): Unit = {
      val offsetDiff = (newOffset.offset - lastOffsetCommitted.offset).toInt

      if (offsetDiff < 0) {
        sys.error(s"Offsets committed out of order: $lastOffsetCommitted followed by $end")
      }

      batches.trimStart(offsetDiff)
      lastOffsetCommitted = newOffset
    }

    LongOffset.convert(end) match {
      case Some(lo) => check(lo)
      case None => sys.error(s"MemoryStream.commit() received an offset ($end) " +
        "that did not originate with an instance of this class")
    }
  }

  override def stop() {}

  def reset(): Unit = synchronized {
    batches.clear()
    currentOffset = new LongOffset(-1)
    lastOffsetCommitted = new LongOffset(-1)
  }
}

class MemoryStreamV2Reader[A: Encoder](
  val schema: StructType,
  data: Seq[Seq[A]],
  numPartitions: Int = 4)
  extends DataSourceV2Reader with Logging {

  override def readSchema(): StructType = schema

  override def createReadTasks(): java.util.List[ReadTask[Row]] = {
    data.flatten
      .grouped(numPartitions)
      .map(dataGroup => new MemoryStreamReadTask(dataGroup).asInstanceOf[ReadTask[Row]])
      .toSeq.asJava
  }
}

class MemoryStreamReadTask[A: Encoder](data: Seq[A]) extends ReadTask[Row] {
  override def createDataReader(): DataReader[Row] = new MemoryStreamDataReader(data)
}

class MemoryStreamDataReader[A: Encoder](data: Seq[A]) extends DataReader[Row] {
  private val iter = data.iterator
  private var currentVal: Row = null

  override def get(): Row = currentVal

  override def next(): Boolean = {
    if (iter.hasNext) {
      currentVal = Row(iter.next())
      true
    } else {
      currentVal = null
      false
    }
  }

  override def close(): Unit = {}
}

/**
 * A sink that stores the results in memory. This [[Sink]] is primarily intended for use in unit
 * tests and does not provide durability.
 */
class MemorySinkV2 extends DataSourceV2 with WriteSupport with Logging {
  override def createWriter(
      batchId: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceV2Options): java.util.Optional[DataSourceV2Writer] = {
    val outputMode = mode match {
      case SaveMode.Append => OutputMode.Append()
      case SaveMode.Overwrite => OutputMode.Complete()
      case _ => throw new IllegalArgumentException("unsupported save mode")
    }
    java.util.Optional.of(new MemoryWriter(this, batchId.asInstanceOf[Int], outputMode))
  }

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  def write(batchId: Int, outputMode: OutputMode, newRows: Array[Row]): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, newRows)
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, newRows)
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySink"
}

case class MemoryWriterCommitMessage(partition: Int, data: Seq[Row]) extends WriterCommitMessage {}

class MemoryWriter(sink: MemorySinkV2, batchId: Int, outputMode: OutputMode)
  extends DataSourceV2Writer with Logging {

  override def createWriterFactory: MemoryWriterFactory = MemoryWriterFactory(outputMode)

  def commit(messages: Array[WriterCommitMessage]): Unit = {
    val newRows = messages.flatMap { message =>
      assert(message.isInstanceOf[MemoryWriterCommitMessage])
      message.asInstanceOf[MemoryWriterCommitMessage].data
    }
    sink.write(batchId, outputMode, newRows.toArray)
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // Don't accept any of the new input.
  }
}

case class MemoryWriterFactory(outputMode: OutputMode) extends DataWriterFactory[Row] {
  def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new MemoryDataWriter(partitionId, outputMode)
  }
}

class MemoryDataWriter(partition: Int, outputMode: OutputMode)
  extends DataWriter[Row] with Logging {

  private case class AddedData(batchId: Long, data: Array[Row])

  /** An order list of batches that have been written to this [[Sink]]. */
  @GuardedBy("this")
  private val batches = new ArrayBuffer[AddedData]()

  /** Returns all rows that are stored in this [[Sink]]. */
  def allData: Seq[Row] = synchronized {
    batches.map(_.data).flatten
  }

  def latestBatchId: Option[Long] = synchronized {
    batches.lastOption.map(_.batchId)
  }

  def latestBatchData: Seq[Row] = synchronized { batches.lastOption.toSeq.flatten(_.data) }

  def toDebugString: String = synchronized {
    batches.map { case AddedData(batchId, data) =>
      val dataStr = try data.mkString(" ") catch {
        case NonFatal(e) => "[Error converting to string]"
      }
      s"$batchId: $dataStr"
    }.mkString("\n")
  }

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val notCommitted = synchronized {
      latestBatchId.isEmpty || batchId > latestBatchId.get
    }
    if (notCommitted) {
      logDebug(s"Committing batch $batchId to $this")
      outputMode match {
        case Append | Update =>
          val rows = AddedData(batchId, data.collect())
          synchronized { batches += rows }

        case Complete =>
          val rows = AddedData(batchId, data.collect())
          synchronized {
            batches.clear()
            batches += rows
          }

        case _ =>
          throw new IllegalArgumentException(
            s"Output mode $outputMode is not supported by MemorySink")
      }
    } else {
      logDebug(s"Skipping already committed batch: $batchId")
    }
  }

  def clear(): Unit = synchronized {
    batches.clear()
  }

  override def toString(): String = "MemorySink"
}

