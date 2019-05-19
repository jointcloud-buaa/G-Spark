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

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages.NetworkMetricDaemonStateChanged
import org.apache.spark.deploy.globalmaster.NetworkMetricDaemonState
import org.apache.spark.deploy.globalmaster.NetworkMetricDaemonState.NetworkMetricDaemonState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

class NetworkMetricRunner(
  siteMasterId: String,
  val siteMaster: RpcEndpointRef,
  metricDir: File,
  conf: SparkConf,
  @volatile var state: NetworkMetricDaemonState
) extends Logging {

  private var workerThread: Thread = _
  private var process: Process = _
  private var stdoutAppender: FileAppender = _
  private var stderrAppender: FileAppender = _
  private var shutdownHook: AnyRef = _

  private var NETWORK_METRIC_TERMINATE_TIMEOUT_MS = 10 * 1000

  def start(): Unit = {
    workerThread = new Thread("network metric daemon") {
      override def run(): Unit = runMetricDaemon()
    }
    workerThread.start()
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      if (state == NetworkMetricDaemonState.RUNNING) {
        state = NetworkMetricDaemonState.FAILED
      }
      killProcess(Some("SiteMaster shutting down"))
    }
  }

  private def killProcess(msg: Option[String]): Unit = {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo(s"Killing metric daemon process for reason: $msg")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, NETWORK_METRIC_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      } else {
        logInfo(s"Successed to terminate process with ${exitCode.get}")
      }
    }
    try {
      siteMaster.send(NetworkMetricDaemonStateChanged(
        siteMasterId, state, msg, exitCode
      ))
    } catch {
      case e: IllegalStateException =>
        logWarning(e.getMessage, e)
    }
  }

  def kill(): Unit = {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread = null
      state = NetworkMetricDaemonState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  private def runMetricDaemon(): Unit = {
    try {
      val cmd = "/usr/sbin/bwctld -c /etc/bwctl -Z"
      val builder = new ProcessBuilder("/bin/bash", "-c", cmd)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")
      builder.directory(metricDir)

      process = builder.start()
      val stdout = new File(metricDir, "network-metric-stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)
      val stderr = new File(metricDir, "network-metric-stderr")
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      // 需要在此处发送给GM表示已经启动，否则，waitFor会阻塞
      siteMaster.send(NetworkMetricDaemonStateChanged(siteMasterId, state, None, None))

      val exitCode = process.waitFor()

      state = NetworkMetricDaemonState.EXITED
      val msg = s"Command exited with code $exitCode"
      siteMaster.send(NetworkMetricDaemonStateChanged(
        siteMasterId, state, Some(msg), Some(exitCode)
      ))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for network metric daemon interrupted")
        state = NetworkMetricDaemonState.KILLED
        killProcess(None)
      case e: Exception =>
        logError("Error running site driver", e)
        state = NetworkMetricDaemonState.FAILED
        killProcess(Some(e.toString))
    }
  }

}
