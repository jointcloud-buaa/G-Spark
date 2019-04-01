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
package org.apache.spark.siteDriver

import org.apache.spark.scheduler.{ExecutorLossReason, TaskSet}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

private[spark] trait SiteTaskScheduler {

  def start(): Unit
  def stop(): Unit
  def postStartHook(): Unit

  // 在当前集群提交任务
  def submitTasks(taskSet: TaskSet): Unit

  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  // 接受来自executor的心跳
  def executorHeartbeatReceived(
    execId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId): Boolean

  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  def siteAppId(): String

  def defaultParallelism(): Int
}
