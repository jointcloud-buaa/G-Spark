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

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{ApplicationDescription, CommandUtils}
import org.apache.spark.deploy.DeployMessages.SiteDriverStateChanged
import org.apache.spark.deploy.globalmaster.SiteDriverState
import org.apache.spark.deploy.globalmaster.SiteDriverState.SiteDriverState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.util.logging.FileAppender

private[deploy] class SiteDriverRunner(
  val appId: String,
  val sdId: String,
  val appDesc: ApplicationDescription,
  val cores: Int,
  val memory: Int,
  val siteMaster: RpcEndpointRef,
  val siteMasterId: String,
  val host: String,
  val webUiPort: Int,
  val publicAddress: String,
  val sparkHome: File,
  val siteDriverDir: File,
  val siteMasterUrl: String,
  conf: SparkConf,
  appLocalDirs: Seq[String],
  @volatile var state: SiteDriverState
) extends Logging {

  private val fullId = appId + "/" + sdId
  private var workerThread: Thread = null
  private var process: Process = null
  private var stdoutAppender: FileAppender = null
  private var stderrAppender: FileAppender = null

  private var SITE_DRIVER_TERMINATE_TIMEOUT_MS = 10 * 1000

  private var shutdownHook: AnyRef = null

  private[sitemaster] def start(): Unit = {
    workerThread = new Thread("SiteDriverRunner for " + fullId) {
      override def run() { fetchAndRunExecutor() }
    }
    workerThread.start()
    shutdownHook = ShutdownHookManager.addShutdownHook { () =>
      if (state == SiteDriverState.RUNNING) {
        state = SiteDriverState.FAILED
      }
      killProcess(Some("SiteMaster shutting down"))
    }
  }

  private def killProcess(msg: Option[String]): Unit = {
    var exitCode: Option[Int] = None
    if (process != null) {
      logInfo("Killing process")
      if (stdoutAppender != null) {
        stdoutAppender.stop()
      }
      if (stderrAppender != null) {
        stderrAppender.stop()
      }
      exitCode = Utils.terminateProcess(process, SITE_DRIVER_TERMINATE_TIMEOUT_MS)
      if (exitCode.isEmpty) {
        logWarning("Failed to terminate process: " + process +
          ". This process will likely be orphaned.")
      }
    }
    try {
      siteMaster.send(SiteDriverStateChanged(appId, sdId, state, msg, exitCode))
    } catch {
      case e: IllegalStateException =>
        logWarning(e.getMessage, e)
    }
  }

  def kill(): Unit = {
    if (workerThread != null) {
      workerThread.interrupt()
      workerThread = null
      state = SiteDriverState.KILLED
      try {
        ShutdownHookManager.removeShutdownHook(shutdownHook)
      } catch {
        case e: IllegalStateException => None
      }
    }
  }

  private[sitemaster] def substituteVariables(argument: String): String = argument match {
    case "{{SITE_MASTER_URL}}" => siteMasterUrl
    case "{{SITE_DRIVER_ID}}" => sdId
    case "{{HOSTNAME}}" => host
    case "{{CORES}}" => cores.toString
    case "{{APP_ID}}" => appId
    case other => other
  }

  private def fetchAndRunExecutor(): Unit = {
    try {
      val builder = CommandUtils.buildProcessBuilder(appDesc.command, new SecurityManager(conf),
        memory, sparkHome.getAbsolutePath, substituteVariables)
      val command = builder.command()
      val formattedCommand = command.asScala.mkString("\"", "\" \"", "\"")
      logInfo(s"Launch command: $formattedCommand")

      builder.directory(siteDriverDir)
      builder.environment.put("SPARK_SITE_DRIVER_DIRS", appLocalDirs.mkString(File.pathSeparator))

      val baseUrl =
        if (conf.getBoolean("spark.ui.reverseProxy", false)) {
          s"/proxy/$siteMasterId/logPage/?appId=$appId&siteDriverId=$sdId&logType="
        } else {
          s"http://$publicAddress:$webUiPort/logPage/?appId=$appId&siteDriverId=$sdId&logType="
        }
      builder.environment.put("SPARK_LOG_URL_STDERR", s"${baseUrl}stderr")
      builder.environment.put("SPARK_LOG_URL_STDOUT", s"${baseUrl}stdout")

      process = builder.start()
      val header = "Spark SiteDriver Command: %s\n%s\n\n".format(
        formattedCommand, "=" * 40
      )

      val stdout = new File(siteDriverDir, "stdout")
      stdoutAppender = FileAppender(process.getInputStream, stdout, conf)
      val stderr = new File(siteDriverDir, "stderr")
      stderrAppender = FileAppender(process.getErrorStream, stderr, conf)

      val exitCode = process.waitFor()

      state = SiteDriverState.EXITED
      val msg = "Command exited with code " + exitCode
      siteMaster.send(SiteDriverStateChanged(appId, sdId, state, Some(msg), Some(exitCode)))
    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for site driver " + fullId + " interrupted")
        state = SiteDriverState.KILLED
        killProcess(None)
      case e: Exception =>
        logError("Error running site driver", e)
        state = SiteDriverState.FAILED
        killProcess(Some(e.toString))
    }
  }
}
