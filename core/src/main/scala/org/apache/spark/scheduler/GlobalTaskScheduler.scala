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
package org.apache.spark.scheduler

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

private[spark] trait GlobalTaskScheduler {
  private val appId = "spark-application-" + System.currentTimeMillis

  // 根调度队列
  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  // 在系统成功初始化后调用. 可以理解为, 在此处等待应用的执行架构启动成功, 此后即可分配task
  def postStartHook() { }

  // Disconnect from the cluster.
  def stop(): Unit

  // TODO-lzp: 具体的每个stage的task如何提交尚不明确
  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit

  // Cancel a stage.
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  // 在submitTasks调用前会调用此来设置DAGScheduler
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // TODO-lzp: 两层架构下的默认并行度如何定义
  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int

  // 周期性更新此site driver上task的一些统计信息, 并检测site driver's blockmanager是否活着
  def siteDriverHeartbeatReceived(
    sdriverId: String,
    accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
    blockManagerId: BlockManagerId): Boolean

  def applicationId(): String = appId

  def siteDriverLost(sdriverId: String, reason: SiteDriverLossReason): Unit

  def applicationAttemptId(): Option[String]

}
