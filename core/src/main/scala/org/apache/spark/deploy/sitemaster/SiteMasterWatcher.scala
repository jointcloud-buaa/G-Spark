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

package org.apache.spark.deploy.sitemaster

import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEndpoint, RpcEnv}

/**
 * Endpoint which connects to a siteMaster process and terminates the JVM if the
 * connection is severed.
 *
 * Provides fate sharing between a siteMaster and its associated child processes.
 */
class SiteMasterWatcher(
  override val rpcEnv: RpcEnv, siteMasterUrl: String, isTesting: Boolean = false)
  extends RpcEndpoint with Logging{

  logInfo(s"Connecting to site master $siteMasterUrl")
  if (!isTesting) {
    rpcEnv.asyncSetupEndpointRefByURI(siteMasterUrl)
  }

  private[deploy] var isShutdown = false

  private val expectedAddress = RpcAddress.fromURIString(siteMasterUrl)
  private def isSiteMaster(address: RpcAddress) = expectedAddress == address

  private def exitNonZero() = if (isTesting) isShutdown = true else System.exit(-1)

  override def receive: PartialFunction[Any, Unit] = {
    case e => logWarning(s"Received unexpected message: $e")
  }

  override def onConnected(remoteAddress: RpcAddress): Unit = {
    if (isSiteMaster(remoteAddress)) {
      logInfo(s"Successfully connected to $siteMasterUrl")
    }
  }

  override def onDisconnected(remoteAddress: RpcAddress): Unit = {
    if (isSiteMaster(remoteAddress)) {
      // This log message will never be seen
      logError(s"Lost connection to site master rpc endpoint $siteMasterUrl. Exiting.")
      exitNonZero()
    }
  }

  override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    if (isSiteMaster(remoteAddress)) {
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(s"Could not initialize connection to worker $siteMasterUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()
    }
  }
}
