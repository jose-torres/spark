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

package org.apache.spark.sql.execution.streaming.sources

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.sources.v2.DataSourceV2Options
import org.apache.spark.sql.sources.v2.streaming.writer.StreamWriter
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/** Common methods used to create writes for the the console sink */
class ConsoleWriter(schema: StructType, options: DataSourceV2Options)
    extends StreamWriter with Logging {

  // Number of rows to display, by default 20 rows
  protected val numRowsToShow = options.getInt("numRows", 20)

  // Truncate the displayed data if it is too long, by default it is true
  protected val isTruncated = options.getBoolean("truncate", true)

  assert(SparkSession.getActiveSession.isDefined)
  protected val spark = SparkSession.getActiveSession.get

  def createWriterFactory(): DataWriterFactory[Row] = PackedRowWriterFactory

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
    // We have to print a "Batch" label for the epoch for compatibility with the pre-data source V2
    // behavior.
    printRows(messages, schema, s"Batch: $epochId")
  }

  def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}

  protected def printRows(
      commitMessages: Array[WriterCommitMessage],
      schema: StructType,
      printMessage: String): Unit = {
    val rows = commitMessages.collect {
      case PackedRowCommitMessage(rs) => rs
    }.flatten

    // scalastyle:off println
    println("-------------------------------------------")
    println(printMessage)
    println("-------------------------------------------")
    // scalastyle:off println
    spark
      .createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .show(numRowsToShow, isTruncated)
  }

  override def toString(): String = {
    s"ConsoleWriter[numRows=$numRowsToShow, truncate=$isTruncated]"
  }
}
