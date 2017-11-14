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

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.ContinuousExecution
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

/** Helper object used to create reference to [[EpochCoordinator]]. */
object EpochCoordinatorRef extends Logging {

  private val endpointNamePrefix = "EpochCoordinator-"

  private def endpointName(queryId: String) = s"EpochCoordinator-$queryId"

  /**
   * Create a reference to an [[EpochCoordinator]]
   */
  def forDriver(
      writer: ContinuousWriter,
      queryId: String,
      session: SparkSession,
      env: SparkEnv): RpcEndpointRef = synchronized {
    try {
      val coordinator = new EpochCoordinator(writer, session, env.rpcEnv)
      val coordinatorRef = env.rpcEnv.setupEndpoint(endpointName(queryId), coordinator)
      logInfo("Registered EpochCoordinator endpoint")
      coordinatorRef
    } catch {
      case e: IllegalArgumentException =>
        val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName(queryId), env.conf, env.rpcEnv)
        logDebug("Retrieved existing EpochCoordinator endpoint")
        rpcEndpointRef
    }
  }

  def forExecutor(queryId: String, env: SparkEnv): RpcEndpointRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName(queryId), env.conf, env.rpcEnv)
    logDebug("Retrieved existing EpochCoordinator endpoint")
    rpcEndpointRef
  }
}

class EpochCoordinator(writer: ContinuousWriter, session: SparkSession, override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val startTime = System.currentTimeMillis()

  private val latestCommittedEpoch = 0

  // (epoch, partition) -> message
  // This is small enough that we don't worry too much about optimizing the shape of the structure.
  private val partitionCommits =
    mutable.Map[(Long, Int), WriterCommitMessage]()

  private def storeWriterCommit(epoch: Long, partition: Int, message: WriterCommitMessage): Unit = {
    if (!partitionCommits.isDefinedAt((epoch, partition))) {
      partitionCommits.put((epoch, partition), message)
      val thisEpochCommits =
        partitionCommits.collect { case ((e, _), msg) if e == epoch => msg }
      if (thisEpochCommits.size == 3) {
        logError(s"Epoch $epoch has received commits from all partitions. Committing to writer.")
        writer.commit(epoch, thisEpochCommits.toArray)
      }
    }
  }

  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logError(s"Got commit from partition $partitionId at epoch $epoch: $message")
      storeWriterCommit(epoch, partitionId, message)
    case ReportPartitionOffset(partitionId, epoch, offsetJson) =>
      val streams = session.streams
      val query = streams.get(writer.getQueryId).asInstanceOf[ContinuousExecution]
      query.addEpochOffset(partitionId, epoch, offsetJson)
      logError(s"Got offset from partition $partitionId at epoch $epoch: $offsetJson")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCurrentEpoch() =>
      val epoch = (System.currentTimeMillis() - startTime) / 999
      logDebug(s"Epoch $epoch")
      context.reply(epoch)
  }
}
