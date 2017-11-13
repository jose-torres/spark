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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcCallContext, RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.sources.v2.writer.{ContinuousWriter, WriterCommitMessage}
import org.apache.spark.util.RpcUtils

case class CommitPartitionEpoch(
  partitionId: Int,
  epoch: Long,
  message: WriterCommitMessage)

case class GetCurrentEpoch()

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
      env: SparkEnv): RpcEndpointRef = synchronized {
    try {
      val coordinator = new EpochCoordinator(writer, queryId, env.rpcEnv)
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

class EpochCoordinator(writer: ContinuousWriter, queryId: String, override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {

  private val startTime = System.currentTimeMillis()

  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logError(s"Got commit from partition $partitionId at epoch $epoch: $message")
      writer.commit(epoch, Seq(message).toArray)
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetCurrentEpoch() =>
      val epoch = (System.currentTimeMillis() - startTime) / 500
      logDebug(s"Epoch $epoch")
      context.reply(epoch)
  }
}
