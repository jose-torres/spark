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

package org.apache.spark.sql.kafka010

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.scalatest.time.SpanSugar._
import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.util.Utils

/**
 * This is a temporary port of KafkaSinkSuite, since we do not yet have a V2 memory stream.
 * Once we have one, this will be changed to a specialization of KafkaSinkSuite and we won't have
 * to duplicate all the code.
 */
class KafkaContinuousSinkSuite extends KafkaContinuousTest {
  import testImplicits._

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils(
      withBrokerProps = Map("auto.create.topics.enable" -> "false"))
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("streaming - write to kafka with topic field") {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 1)

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()

    val topic = newTopic()
    testUtils.createTopic(topic)

    val writer = createKafkaWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as topic", "value")

    val reader = createKafkaReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
      .as[(Int, Int)]
      .map(_._2)

    try {
      testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      testUtils.sendMessages(inputTopic, Array("6", "7", "8", "9", "10"))
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write data with bad schema") {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 1)

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()
    val topic = newTopic()
    testUtils.createTopic(topic)

    /* No topic field or topic option */
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = "value as key", "value"
        )
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("topic option required when no 'topic' attribute is present"))

    try {
      /* No value field */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "value as key"
        )
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "required attribute 'value' not found"))
  }

  test("streaming - write data with valid schema but wrong types") {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 1)

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value as STRING) value")
    val topic = newTopic()
    testUtils.createTopic(topic)

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* topic field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"CAST('1' as INT) as topic", "value"
        )
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("topic type must be a string"))

    try {
      /* value field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as value"
        )
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "value attribute type must be a string or binarytype"))

    try {
      ex = intercept[StreamingQueryException] {
        /* key field wrong type */
        writer = createKafkaWriter(input.toDF())(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as key", "value"
        )
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "key attribute type must be a string or binarytype"))
  }

  test("streaming - write to non-existing topic") {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 1)

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "earliest")
      .load()
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      ex = intercept[StreamingQueryException] {
        writer = createKafkaWriter(input.toDF(), withTopic = Some(topic))()
        testUtils.sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
        eventually(timeout(streamingTimeout)) {
          assert(writer.exception.isDefined)
        }
        throw writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("job aborted"))
  }

  test("streaming - exception on config serializer") {
    val inputTopic = newTopic()
    testUtils.createTopic(inputTopic, partitions = 1)
    testUtils.sendMessages(inputTopic, Array("0"))

    val input = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", inputTopic)
      .load()
    var writer: StreamingQuery = null
    var ex: Exception = null
    ex = intercept[StreamingQueryException] {
      writer = createKafkaWriter(
        input.toDF(),
        withOptions = Map("kafka.key.serializer" -> "foo"))()
      writer.processAllAvailable()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "kafka option 'key.serializer' is not supported"))

    ex = intercept[StreamingQueryException] {
      writer = createKafkaWriter(
        input.toDF(),
        withOptions = Map("kafka.value.serializer" -> "foo"))()
      writer.processAllAvailable()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "kafka option 'value.serializer' is not supported"))
  }

  test("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Kafka when
    * is comes to blocking on a call to send when the send buffer is full.
    * This test will configure the smallest possible producer buffer and
    * indicate that we should block when it is full. Thus, no exception should
    * be thrown in the case of a full buffer.
    */
    val topic = newTopic()
    testUtils.createTopic(topic, 1)
    val options = new java.util.HashMap[String, String]
    options.put("bootstrap.servers", testUtils.brokerAddress)
    options.put("buffer.memory", "16384") // min buffer size
    options.put("block.on.buffer.full", "true")
    options.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    options.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new KafkaContinuousDataWriter(Some(topic), options.asScala.toMap, inputSchema)
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      iter.foreach(writeTask.write(_))
      writeTask.commit()
    } finally {
      writeTask.close()
    }
  }

  private def createKafkaReader(topic: String): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .option("subscribe", topic)
      .load()
  }

  private def createKafkaWriter(
      input: DataFrame,
      withTopic: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    val checkpointDir = Utils.createTempDir()
    var df = input.toDF()
    if (withSelectExpr.length > 0) {
      df = df.selectExpr(withSelectExpr: _*)
    }
    stream = df.writeStream
      .format("kafka")
      .option("checkpointLocation", checkpointDir.getCanonicalPath)
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      // We need to reduce blocking time to efficiently test non-existent partition behavior.
      .option("kafka.max.block.ms", "1000")
      .trigger(Trigger.Continuous(1000))
      .queryName("kafkaStream")
    withTopic.foreach(stream.option("topic", _))
    withOutputMode.foreach(stream.outputMode(_))
    withOptions.foreach(opt => stream.option(opt._1, opt._2))
    stream.start()
  }
}
