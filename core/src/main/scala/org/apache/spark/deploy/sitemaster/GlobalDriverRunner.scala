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

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{CommandUtils, GlobalDriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.DeployMessages.GlobalDriverStateChanged
import org.apache.spark.deploy.globalmaster.GlobalDriverState
import org.apache.spark.deploy.globalmaster.GlobalDriverState.GlobalDriverState
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.{Clock, ShutdownHookManager, SystemClock, Utils}

/**
 * Manages the execution of one driver, including automatically restarting the driver on failure.
 * This is currently only used in standalone cluster deploy mode.
 */
private[deploy] class GlobalDriverRunner(
    conf: SparkConf,
    val gdriverId: String,
    val workDir: File,
    val sparkHome: File,
    val gdriverDesc: GlobalDriverDescription,
    val siteMaster: RpcEndpointRef,
    val siteMasterUrl: String,
    val securityManager: SecurityManager)
  extends Logging {

  @volatile private var process: Option[Process] = None
  @volatile private var killed = false

  // Populated once finished
  @volatile private[sitemaster] var finalState: Option[GlobalDriverState] = None
  @volatile private[sitemaster] var finalException: Option[Exception] = None

  // Timeout to wait for when trying to terminate a driver.
  private val GLOBAL_DRIVER_TERMINATE_TIMEOUT_MS =
    conf.getTimeAsMs("spark.siteMaster.globalDriverTerminateTimeout", "10s")

  // Decoupled for testing
  def setClock(_clock: Clock): Unit = {
    clock = _clock
  }

  def setSleeper(_sleeper: Sleeper): Unit = {
    sleeper = _sleeper
  }

  private var clock: Clock = new SystemClock()
  private var sleeper = new Sleeper {
    def sleep(seconds: Int): Unit = (0 until seconds).takeWhile { _ =>
      Thread.sleep(1000)
      !killed
    }
  }

  /** Starts a thread to run and manage the driver. */
  private[sitemaster] def start() = {
    new Thread("GlobalDriverRunner for " + gdriverId) {
      override def run() {
        var shutdownHook: AnyRef = null
        try {
          shutdownHook = ShutdownHookManager.addShutdownHook { () =>
            logInfo(s"Worker shutting down, killing driver $gdriverId")
            kill()
          }

          // prepare driver jars and run driver
          val exitCode = prepareAndRunDriver()

          // set final state depending on if forcibly killed and process exit code
          finalState = if (exitCode == 0) {
            Some(GlobalDriverState.FINISHED)
          } else if (killed) {
            Some(GlobalDriverState.KILLED)
          } else {
            Some(GlobalDriverState.FAILED)
          }
        } catch {
          case e: Exception =>
            kill()
            finalState = Some(GlobalDriverState.ERROR)
            finalException = Some(e)
        } finally {
          if (shutdownHook != null) {
            ShutdownHookManager.removeShutdownHook(shutdownHook)
          }
        }

        // notify worker of final driver state, possible exception
        siteMaster.send(GlobalDriverStateChanged(gdriverId, finalState.get, finalException))
      }
    }.start()
  }

  /** Terminate this driver (or prevent it from ever starting if not yet started) */
  private[sitemaster] def kill(): Unit = {
    logInfo("Killing driver process!")
    killed = true
    synchronized {
      process.foreach { p =>
        val exitCode = Utils.terminateProcess(p, GLOBAL_DRIVER_TERMINATE_TIMEOUT_MS)
        if (exitCode.isEmpty) {
          logWarning("Failed to terminate driver process: " + p +
              ". This process will likely be orphaned.")
        }
      }
    }
  }

  /**
   * Creates the working directory for this driver.
   * Will throw an exception if there are errors preparing the directory.
   */
  private def createWorkingDirectory(): File = {
    val gdriverDir = new File(workDir, gdriverId)
    if (!gdriverDir.exists() && !gdriverDir.mkdirs()) {
      throw new IOException("Failed to create directory " + gdriverDir)
    }
    gdriverDir
  }

  /**
   * Download the user jar into the supplied directory and return its local path.
   * Will throw an exception if there are errors downloading the jar.
   */
  private def downloadUserJar(driverDir: File): String = {
    val jarFileName = new URI(gdriverDesc.jarUrl).getPath.split("/").last
    val localJarFile = new File(driverDir, jarFileName)
    if (!localJarFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user jar ${gdriverDesc.jarUrl} to $localJarFile")
      Utils.fetchFile(
        gdriverDesc.jarUrl,
        driverDir,
        conf,
        securityManager,
        SparkHadoopUtil.get.newConfiguration(conf),
        System.currentTimeMillis(),
        useCache = false)
      if (!localJarFile.exists()) { // Verify copy succeeded
        throw new IOException(
          s"Can not find expected jar $jarFileName which should have been loaded in $driverDir")
      }
    }
    localJarFile.getAbsolutePath
  }

  private[sitemaster] def prepareAndRunDriver(): Int = {
    val gdriverDir = createWorkingDirectory()
    val localJarFilename = downloadUserJar(gdriverDir)

    def substituteVariables(argument: String): String = argument match {
      case "{{SITE_MASTER_URL}}" => siteMasterUrl
      case "{{USER_JAR}}" => localJarFilename
      case other => other
    }

    // TODO: If we add ability to submit multiple jars they should also be added here
    val builder = CommandUtils.buildProcessBuilder(gdriverDesc.command, securityManager,
      gdriverDesc.mem, sparkHome.getAbsolutePath, substituteVariables)

    runDriver(builder, gdriverDir, gdriverDesc.supervise)
  }

  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    builder.directory(baseDir)
    // 重定向输出流和错误流
    def initialize(process: Process): Unit = {
      // Redirect stdout and stderr to files
      val stdout = new File(baseDir, "stdout")
      CommandUtils.redirectStream(process.getInputStream, stdout)

      val stderr = new File(baseDir, "stderr")
      val formattedCommand = builder.command.asScala.mkString("\"", "\" \"", "\"")
      val header = "Launch Command: %s\n%s\n\n".format(formattedCommand, "=" * 40)
      Files.append(header, stderr, StandardCharsets.UTF_8)
      CommandUtils.redirectStream(process.getErrorStream, stderr)
    }
    runCommandWithRetry(ProcessBuilderLike(builder), initialize, supervise)
  }

  private[sitemaster] def runCommandWithRetry(
      command: ProcessBuilderLike, initialize: Process => Unit, supervise: Boolean): Int = {
    var exitCode = -1
    // Time to wait between submission retries.
    var waitSeconds = 1
    // A run of this many seconds resets the exponential back-off.
    val successfulRunDuration = 5
    var keepTrying = !killed

    while (keepTrying) {
      logInfo("Launch Command: " + command.command.mkString("\"", "\" \"", "\""))

      synchronized {
        if (killed) { return exitCode }
        process = Some(command.start())
        initialize(process.get)
      }

      val processStart = clock.getTimeMillis()
      exitCode = process.get.waitFor()  // 阻塞

      // check if attempting another run
      keepTrying = supervise && exitCode != 0 && !killed
      if (keepTrying) {
        if (clock.getTimeMillis() - processStart > successfulRunDuration * 1000) {
          waitSeconds = 1
        }
        logInfo(s"Command exited with status $exitCode, re-launching after $waitSeconds s.")
        sleeper.sleep(waitSeconds)
        waitSeconds = waitSeconds * 2 // exponential back-off
      }
    }

    exitCode
  }
}

private[deploy] trait Sleeper {
  def sleep(seconds: Int): Unit
}

// Needed because ProcessBuilder is a final class and cannot be mocked
private[deploy] trait ProcessBuilderLike {
  def start(): Process
  def command: Seq[String]
}

private[deploy] object ProcessBuilderLike {
  def apply(processBuilder: ProcessBuilder): ProcessBuilderLike = new ProcessBuilderLike {
    override def start(): Process = processBuilder.start()
    override def command: Seq[String] = processBuilder.command().asScala
  }
}
