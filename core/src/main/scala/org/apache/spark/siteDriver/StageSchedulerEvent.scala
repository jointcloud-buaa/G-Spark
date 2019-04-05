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

import scala.language.existentials

import org.apache.spark.TaskEndReason
import org.apache.spark.scheduler.{ExecutorLossReason, Task, TaskInfo, TaskSet}
import org.apache.spark.util.AccumulatorV2

private[siteDriver] sealed trait StageSchedulerEvent

// 执行器添加
private[siteDriver] case class ExecutorAdded(execId: String, host: String)
  extends StageSchedulerEvent

// 执行器缺失
private[siteDriver] case class ExecutorLost(execId: String, reason: ExecutorLossReason)
  extends StageSchedulerEvent

// 开始事件
private[siteDriver] case class BeginEvent(task: Task[_], taskInfo: TaskInfo)
  extends StageSchedulerEvent

// 完成事件
private[siteDriver] case class CompletionEvent(
  task: Task[_],
  reason: TaskEndReason,
  result: Any,
  accumUpdates: Seq[AccumulatorV2[_, _]],
  taskInfo: TaskInfo)
  extends StageSchedulerEvent

// 任务集失败
private[siteDriver] case class TaskSetFailed(
  taskSet: TaskSet, reason: String, exception: Option[Throwable]) extends StageSchedulerEvent

// 获取结果事件
private[siteDriver] case class GettingResultEvent(taskInfo: TaskInfo)
  extends StageSchedulerEvent

// 重新提交失败阶段
private[siteDriver] case object ResubmitFailedStages extends StageSchedulerEvent

// 取消阶段
private[siteDriver] case class StageCancelled(stageId: Int) extends StageSchedulerEvent
