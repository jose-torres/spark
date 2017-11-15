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

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{ContinuousExecution, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.ContinuousReader
import org.apache.spark.sql.sources.v2.writer.{ContinuousWriter, WriterCommitMessage}
import org.apache.spark.util.RpcUtils

case class CommitPartitionEpoch(
  partitionId: Int,
  epoch: Long,
  message: WriterCommitMessage)

case class GetCurrentEpoch()

case class ReportPartitionOffset(
  partitionId: Int,
  epoch: Long,
  offsetJson: String)

// Should be used only by ContinuousExecution during initialization.
case class SetEpoch(epoch: Long)

// Should be used only by ContinuousExecution during epoch advancement.
case class IncrementAndGetEpoch()


object LocalCurrentEpochs {
  var epoch: Long = 0
}

/** Helper object used to create reference to [[EpochCoordinator]]. */
object EpochCoordinatorRef extends Logging {

  private val endpointNamePrefix = "EpochCoordinator-"

  private def endpointName(queryId: String) = s"EpochCoordinator-$queryId"

  /**
   * Create a reference to an [[EpochCoordinator]]
   */
  def forDriver(
      writer: ContinuousWriter,
      reader: ContinuousReader,
      queryId: String,
      session: SparkSession,
      env: SparkEnv): RpcEndpointRef = synchronized {
    try {
      val coordinator = new EpochCoordinator(writer, reader, session, env.rpcEnv)
      val ref = env.rpcEnv.setupEndpoint(endpointName(queryId), coordinator)
      logInfo("Registered EpochCoordinator endpoint")
      ref
    } catch {
      case e: IllegalArgumentException =>
        forExecutor(queryId, env)
    }
  }

  def forExecutor(queryId: String, env: SparkEnv): RpcEndpointRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName(queryId), env.conf, env.rpcEnv)
    logDebug("Retrieved existing EpochCoordinator endpoint")
    rpcEndpointRef
  }
}

class EpochCoordinator(writer: ContinuousWriter,
                       reader: ContinuousReader,
                       session: SparkSession,
                       override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val latestCommittedEpoch = 0

  // Should only be mutated by the ContinuousExecution which created this coordinator.
  val epoch = new AtomicLong(0)

  // (epoch, partition) -> message
  // This is small enough that we don't worry too much about optimizing the shape of the structure.
  private val partitionCommits =
    mutable.Map[(Long, Int), WriterCommitMessage]()

  private val partitionOffsets =
    mutable.Map[(Long, Int), String]()

  private def storeWriterCommit(epoch: Long, partition: Int, message: WriterCommitMessage): Unit = {
    // TODO deduplicate and clean this logic
    // we can't assume all the sequencing that's happening here
    // TODO the underlying query commit interaction is broken; it will fail on things that we output
    if (!partitionCommits.isDefinedAt((epoch, partition))) {
      partitionCommits.put((epoch, partition), message)
      val thisEpochCommits =
        partitionCommits.collect { case ((e, _), msg) if e == epoch => msg }
      val nextEpochOffsets =
        partitionOffsets.collect { case ((e, _), o) if e == epoch + 1 => o }
      if (thisEpochCommits.size == 3 && nextEpochOffsets.size == 3) {
        logError(s"Epoch $epoch has received commits from all partitions. Committing globally.")
        // Sequencing is important - writer commits to epoch are required to be replayable
        writer.commit(epoch, thisEpochCommits.toArray)
        val query = session.streams.get(writer.getQueryId).asInstanceOf[ContinuousExecution]
        query.commit(epoch)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logError(s"Got commit from partition $partitionId at epoch $epoch: $message")
      try {
        storeWriterCommit(epoch, partitionId, message)
      } catch {
        case t: Throwable =>
          print(s"EXCEPTION: $t\n")
          throw t
      }

    case ReportPartitionOffset(partitionId, epoch, offsetJson) =>
      try {
        val streams = session.streams
        val query = streams.get(writer.getQueryId).asInstanceOf[ContinuousExecution]
        partitionOffsets.put((epoch, partitionId), offsetJson)
        val thisEpochOffsets =
          partitionOffsets.collect { case ((e, _), o) if e == epoch => o }
        if (thisEpochOffsets.size == 3) {
          logError(s"Epoch $epoch has offsets reported from all partitions: $thisEpochOffsets")
          query.addOffset(epoch, reader, thisEpochOffsets.map(SerializedOffset(_)).toSeq)
        }
        val previousEpochCommits =
          partitionCommits.collect { case ((e, _), msg) if e == epoch - 1 => msg }
        if (previousEpochCommits.size == 3) {
          logError(s"Epoch $epoch has received commits from all partitions. Committing globally.")
          // Sequencing is important - writer commits to epoch are required to be replayable
          writer.commit(epoch, previousEpochCommits.toArray)
          val query = session.streams.get(writer.getQueryId).asInstanceOf[ContinuousExecution]
          query.commit(epoch)
        }
      } catch {
        case t: Throwable =>
          print(s"EXCEPTION: $t\n")
          throw t
      }
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCurrentEpoch() =>
      val result = epoch.get
      logDebug(s"Epoch $result")
      context.reply(result)

    case SetEpoch(newEpoch) =>
      epoch.set(newEpoch)
      context.reply(newEpoch)

    case IncrementAndGetEpoch() =>
      context.reply(epoch.incrementAndGet())
  }
}
