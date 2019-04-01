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

import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.sitemaster.SiteAppState.SiteAppState
import org.apache.spark.rpc.RpcEndpointRef

private[deploy] class SiteAppInfo(
  val startTime: Long,
  val id: String,
  val desc: SiteAppDescription,
  val submitDate: Date,
  val driver: RpcEndpointRef,
  defaultCores: Int
) extends Serializable {

  @transient var state: SiteAppState = _
  @transient var executors: mutable.HashMap[Int, ExecutorDesc] = _
  @transient var removedExecutors: ArrayBuffer[ExecutorDesc] = _
  @transient var coresGranted: Int = 0
  @transient var endTime: Long = _
  @transient var appSource: SiteAppSource = _

  @transient private[sitemaster] var executorLimit: Int = _

  @transient private var nextExecutorId: Int = 0

  init()

  private def init(): Unit = {
    state = SiteAppState.WAITING
    executors = new mutable.HashMap
    coresGranted = 0
    appSource = new SiteAppSource(this)
    nextExecutorId = 0
    removedExecutors = new ArrayBuffer[ExecutorDesc]()
    executorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)
  }

  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  private[sitemaster] def coresLeft: Int = requestedCores - coresGranted

  private var _retryCount = 0

  // TODO-lzp: 感觉retryCount并不能阻止, executor启动无限个, 只要其exit
  private[sitemaster] def retryCount = _retryCount

  private[sitemaster] def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  private[sitemaster] def resetRetryCount(): Unit = _retryCount = 0

  private[sitemaster] def isFinished: Boolean = {
    state != SiteAppState.WAITING && state != SiteAppState.RUNNING
  }

  private def newExecutorId(useID: Option[Int] = None): Int = {
    useID match {
      case Some(id) =>
        nextExecutorId = math.max(nextExecutorId, id + 1)
        id
      case None =>
        val id = nextExecutorId
        nextExecutorId += 1
        id
    }
  }

  private[sitemaster] def addExecutor(
    worker: WorkerInfo,
    cores: Int,
    useID: Option[Int] = None): ExecutorDesc = {
    val exec = new ExecutorDesc(newExecutorId(useID), this, worker, cores, desc.memoryPerExecutorMB)
    executors(exec.id) = exec
    coresGranted += cores
    exec
  }

  private[sitemaster] def removeExecutor(exec: ExecutorDesc) {
    if (executors.contains(exec.id)) {
      removedExecutors += executors(exec.id)
      executors -= exec.id
      coresGranted -= exec.cores
    }
  }

  private[deploy] def getExecutorLimit: Int = executorLimit

  private[sitemaster] def markFinished(endState: SiteAppState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }
}
