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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.kafka.common.TopicPartition
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class Reporter extends StreamingQueryListener {
  override def onQueryStarted(event: QueryStartedEvent): Unit = {}
  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    // scalastyle:off println
    println(event.progress)
    // scalastyle:on println
  }
  override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {}
}

class KafkaLatencySuite extends QueryTest
  with BeforeAndAfter
  with SharedSQLContext
  with MockitoSugar {

  import testImplicits._

  private val topicId = new AtomicInteger(0)

  private var testUtils: KafkaTestUtils = _

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def assignString(topic: String, partitions: Iterable[Int]): String = {
    JsonUtils.partitions(partitions.map(p => new TopicPartition(topic, p)))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    testUtils = new KafkaTestUtils
    testUtils.setup()
  }

  override def afterAll(): Unit = {
    if (testUtils != null) {
      testUtils.teardown()
      testUtils = null
      super.afterAll()
    }
  }

  test("map-only latency test") {
    testUtils.createTopic("input", partitions = 1)
    testUtils.createTopic("results", partitions = 1)

    sql("SET spark.sql.shuffle.partitions=1")

    spark.streams.addListener(new Reporter)

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", "input")
      .option("startingOffsets", "earliest")
      .option("continuous", "true")
      .load()

    val currentTimeMillis = udf(() => System.currentTimeMillis)
    val realTimestamp = udf((time: java.sql.Timestamp) => time.getTime)

    print("starting stream\n")

    val stream = df.select(
      $"value".cast("string").cast("long") as 'creationTime,
      realTimestamp($"timestamp") as 'ingestTime,
      currentTimeMillis() as 'processingTime)
      .select(to_json(struct($"*")) as 'value)
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", "results")
      .option("checkpointLocation", s"/tmp/${java.util.UUID.randomUUID()}")
      .start()

    print("starting input\n")

    spark.range(1, 500, 1, 1)
      .as[Long]
      .map(i => {Thread.sleep(100); i})
      .select(currentTimeMillis().cast("string") as 'value)
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("topic", "input")
      .save()

    print("input complete\n")

    stream.processAllAvailable()

    val schema = new StructType()
      .add("creationTime", LongType)
      .add("ingestTime", LongType)
      .add("processingTime", LongType)

    val results = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("subscribe", "results")
      .option("startingOffsets", "earliest")
      .load()
      .withColumn("record", from_json($"value".cast("string"), schema))
      .select($"record.*", realTimestamp('timestamp) as 'outputTime)

    val latencies = results
      .select(
        $"creationTime" / 1000 cast "timestamp" as 'timestamp,
        $"ingestTime" - $"creationTime" as 'ingestLatency,
        $"processingTime" - $"ingestTime" as 'processingLatency,
        $"outputTime" - $"processingTime" as 'egressLatency,
        $"outputTime" - $"creationTime" as 'totalLatency,
        $"outputTime" - $"ingestTime" as 'kafka2KafkaLatency)

    val latencyAgg = latencies
      .groupBy(window($"timestamp", "10 seconds").getField("start") as 'timeBucket)
      .agg(
        count("*") as 'count,
        format_number(avg($"kafka2KafkaLatency"), 2) as 'kafka2KafkaLatency,
        format_number(avg($"ingestLatency"), 2) as 'ingestLatency,
        format_number(avg($"processingLatency"), 2) as 'processingLatency,
        format_number(avg($"egressLatency"), 2) as 'egressLatency,
        format_number(min($"totalLatency"), 2) as 'minLatency,
        format_number(avg($"totalLatency"), 2) as 'avgLatency,
        format_number(max($"totalLatency"), 2) as 'maxLatency)
      .orderBy($"timeBucket")

    // scalastyle:off println
    println(latencyAgg.schema.map(_.name).mkString("\t"))
    latencyAgg.collect().map(_.mkString("\t")).foreach(println)
    // scalastyle:on println
  }
}
