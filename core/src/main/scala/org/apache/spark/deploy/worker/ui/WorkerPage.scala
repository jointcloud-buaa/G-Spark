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

package org.apache.spark.deploy.worker.ui

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.json4s.JValue

import org.apache.spark.deploy.DeployMessages.{RequestWorkerState, WorkerStateResponse}
import org.apache.spark.deploy.JsonProtocol
import org.apache.spark.deploy.worker.ExecutorRunner
import org.apache.spark.ui.{UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class WorkerPage(parent: WorkerWebUI) extends WebUIPage("") {
  private val workerEndpoint = parent.worker.self

  override def renderJson(request: HttpServletRequest): JValue = {
    val workerState = workerEndpoint.askWithRetry[WorkerStateResponse](RequestWorkerState)
    JsonProtocol.writeWorkerState(workerState)
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val workerState = workerEndpoint.askWithRetry[WorkerStateResponse](RequestWorkerState)

    val executorHeaders = Seq("ExecutorID", "Cores", "State", "Memory", "Job Details", "Logs")
    val runningExecutors = workerState.executors
    val runningExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, runningExecutors)
    val finishedExecutors = workerState.finishedExecutors
    val finishedExecutorTable =
      UIUtils.listingTable(executorHeaders, executorRow, finishedExecutors)

    // For now we only show driver information if the user has submitted drivers to the cluster.
    // This is until we integrate the notion of drivers and applications in the UI.

    val content =
      <div class="row-fluid"> <!-- Worker Details -->
        <div class="span12">
          <ul class="unstyled">
            <li><strong>ID:</strong> {workerState.workerId}</li>
            <li><strong>
              Master URL:</strong> {workerState.masterUrl}
            </li>
            <li><strong>Cores:</strong> {workerState.cores} ({workerState.coresUsed} Used)</li>
            <li><strong>Memory:</strong> {Utils.megabytesToString(workerState.memory)}
              ({Utils.megabytesToString(workerState.memoryUsed)} Used)</li>
          </ul>
          <p><a href={workerState.masterWebUiUrl}>Back to Master</a></p>
        </div>
      </div>
      <div class="row-fluid"> <!-- Executors and Drivers -->
        <div class="span12">
          <h4> Running Executors ({runningExecutors.size}) </h4>
          {runningExecutorTable}
          {
            if (finishedExecutors.nonEmpty) {
              <h4>Finished Executors ({finishedExecutors.size}) </h4> ++
              finishedExecutorTable
            }
          }
        </div>
      </div>;
    UIUtils.basicSparkPage(content, "Spark Worker at %s:%s".format(
      workerState.host, workerState.port))
  }

  def executorRow(executor: ExecutorRunner): Seq[Node] = {
    <tr>
      <td>{executor.execId}</td>
      <td>{executor.cores}</td>
      <td>{executor.state}</td>
      <td sorttable_customkey={executor.memory.toString}>
        {Utils.megabytesToString(executor.memory)}
      </td>
      <td>
        <ul class="unstyled">
          <li><strong>ID:</strong> {executor.siteAppId}</li>
          <li><strong>Name:</strong> {executor.appDesc.name}</li>
          <li><strong>User:</strong> {executor.appDesc.user}</li>
        </ul>
      </td>
      <td>
     <a href={"logPage?appId=%s&executorId=%s&logType=stdout"
        .format(executor.siteAppId, executor.execId)}>stdout</a>
     <a href={"logPage?appId=%s&executorId=%s&logType=stderr"
        .format(executor.siteAppId, executor.execId)}>stderr</a>
      </td>
    </tr>

  }
}
