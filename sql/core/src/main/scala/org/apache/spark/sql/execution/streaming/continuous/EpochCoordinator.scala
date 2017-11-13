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
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.sql.sources.v2.writer.{DataSourceV2Writer, SupportsContinuousWrite, WriterCommitMessage}
import org.apache.spark.util.RpcUtils

case class CommitPartitionEpoch(
  partitionId: Int,
  epoch: Long,
  message: WriterCommitMessage)

/** Helper object used to create reference to [[EpochCoordinator]]. */
object EpochCoordinatorRef extends Logging {

  private val endpointName = "EpochCoordinator"

  /**
   * Create a reference to an [[EpochCoordinator]]
   */
  def forDriver(writer: SupportsContinuousWrite, env: SparkEnv): RpcEndpointRef = synchronized {
    try {
      val coordinator = new EpochCoordinator(writer, env.rpcEnv)
      val coordinatorRef = env.rpcEnv.setupEndpoint(endpointName, coordinator)
      logInfo("Registered EpochCoordinator endpoint")
      coordinatorRef
    } catch {
      case e: IllegalArgumentException =>
        val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
        logDebug("Retrieved existing EpochCoordinator endpoint")
        rpcEndpointRef
    }
  }

  def forExecutor(env: SparkEnv): RpcEndpointRef = synchronized {
    val rpcEndpointRef = RpcUtils.makeDriverRef(endpointName, env.conf, env.rpcEnv)
    logDebug("Retrieved existing EpochCoordinator endpoint")
    rpcEndpointRef
  }
}

class EpochCoordinator(writer: SupportsContinuousWrite, override val rpcEnv: RpcEnv)
  extends ThreadSafeRpcEndpoint with Logging {
  override def receive: PartialFunction[Any, Unit] = {
    case CommitPartitionEpoch(partitionId, epoch, message) =>
      logError(s"Got commit from partition $partitionId at epoch $epoch: $message")
      writer.commit(epoch, Seq(message).toArray)
  }
}
