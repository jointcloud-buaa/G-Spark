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

package org.apache.spark.deploy.client

import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture, TimeoutException, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.ExecutorState
import org.apache.spark.deploy.sitemaster.{SiteAppDescription, SiteMaster}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private[spark] class StandaloneSiteAppClient(
  rpcEnv: RpcEnv,
  siteMasterUrl: String,
  siteAppDescription: SiteAppDescription,
  listener: StandaloneSiteAppClientListener,
  conf: SparkConf
) extends Logging {

  private val siteMasterRpcAddress: RpcAddress = RpcAddress.fromURIString(siteMasterUrl)

  private val REGISTRATION_TIMEOUT_SECONDS = 20
  private val REGISTRATION_RETRIES = 3

  private val endpoint = new AtomicReference[RpcEndpointRef] // appClient
  private val siteAppId = new AtomicReference[String]
  private val registered = new AtomicBoolean(false)

  private val registerMasterThreadPool = ThreadUtils.newDaemonSingleThreadExecutor(
    "site-appclient-register-sitemaster-threadpool"
  )

  // A scheduled executor for scheduling the registration actions
  private val registrationRetryThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "site-appclient-registration-retry-thread"
  )

  def start(): Unit = endpoint.set(
    rpcEnv.setupEndpoint("SiteAppClient", new SiteClientEndpoint(rpcEnv))
  )

  private class SiteClientEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    private var siteMaster: Option[RpcEndpointRef] = None

    // To avoid calling listener.disconnected() multiple times
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    private val alreadyDead = new AtomicBoolean(false)

    private val registerSiteMasterFuture = new AtomicReference[JFuture[_]]
    private val registrationRetryTimer = new AtomicReference[JScheduledFuture[_]]

    override def onStart(): Unit = {
      try {
        registerWithSiteMaster(1)
      } catch {
        case e: Exception =>
          logWarning("Failed to connect to master", e)
          markDisconnected()
          stop()
      }
    }

    override def onStop(): Unit = {
      if (registrationRetryTimer.get != null) {
        registrationRetryTimer.get.cancel(true)
      }
      registrationRetryThread.shutdownNow()
      registerSiteMasterFuture.get.cancel(true)
      registerMasterThreadPool.shutdownNow()
    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredSiteApplication(siteAppId_, smRef) =>
        siteAppId.set(siteAppId_)
        registered.set(true)
        siteMaster = Some(smRef)
        listener.connected(siteAppId.get())

      case SiteAppRemoved(msg) =>
        markDead(s"Site Master removed our site application: $msg")
        stop()

        // from site master, when launch executor
      case ExecutorAdded(id: Int, workerId: String, hostPort: String, cores: Int, memory: Int) =>
        val fullId = s"$siteAppId/$id"
        logInfo("Executor added: %s on %s (%s) with %d cores".format(
          fullId, workerId, hostPort, cores)
        )
        listener.executorAdded(fullId, workerId, hostPort, cores, memory)

        // from site master, when executor state updated, or remove worker
      case ExecutorUpdated(id, state, message, exitStatus, workerLost) =>
        val fullId = siteAppId + "/" + id
        val messageText = message.map(s => " (" + s + ")").getOrElse("")
        logInfo("Executor updated: %s is now %s%s".format(fullId, state, messageText))
        if (ExecutorState.isFinished(state)) {
          listener.executorRemoved(fullId, message.getOrElse(""), exitStatus, workerLost)
        }

      case SiteMasterChanged(smRef, smWebUiUrl) =>
        logInfo("Site Master has changed, new site master is at " + smRef.address.toSparkURL)
        siteMaster = Some(smRef)
        alreadyDisconnected = false
        smRef.send(SiteMasterChangeAcknowledged(siteAppId.get))
    }

    // TODO-lzp: wait to fill code
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case StopSiteAppClient =>
        markDead("Site App has been stopped")
        context.reply(true)
        stop()

      // 自已发送给自己的, 用来请求executors
      case r: RequestExecutors =>
        siteMaster match {
          case Some(m) => askAndReplyAsync(m, context, r)
          case None =>
            logWarning("Attempted to request executors before registering with Master.")
            context.reply(false)
        }

      case k: KillExecutors =>
        siteMaster match {
          case Some(m) => askAndReplyAsync(m, context, k)
          case None =>
            logWarning("Attempted to kill executors before registering with Master.")
            context.reply(false)
        }
    }

    private def askAndReplyAsync[T](
      endpointRef: RpcEndpointRef,
      context: RpcCallContext,
      msg: T): Unit = {
      // Ask a message and create a thread to reply with the result.  Allow thread to be
      // interrupted during shutdown, otherwise context must be notified of NonFatal errors.
      endpointRef.ask[Boolean](msg).andThen {
        case Success(b) => context.reply(b)
        case Failure(ie: InterruptedException) => // Cancelled
        case Failure(NonFatal(t)) => context.sendFailure(t)
      }(ThreadUtils.sameThread)
    }

    override def onNetworkError(cause: Throwable, address: RpcAddress): Unit = {
      if (siteMasterRpcAddress == address) {
        logWarning(s"Could not connect to $address: $cause")
      }
    }

    override def onDisconnected(address: RpcAddress): Unit = {
      if (siteMaster.exists(_.address == address)) {
        logWarning(s"Connection to $address failed; waiting for site master to reconnect...")
        markDisconnected()
      }
    }

    private def registerWithSiteMaster(nthRetry: Int): Unit = {
      registerSiteMasterFuture.set(tryRegisterMaster())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) { // 如果已经注册成功
            registerSiteMasterFuture.get.cancel(true)
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) { // 超过注册次数
            markDead("All masters are unresponsive! Giving up.")
          } else { // 因为是延时执行的, 此时仍然没有成功, 则取消之前的结果, 重试
            registerSiteMasterFuture.get.cancel(true)
            registerWithSiteMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) // 延时多长时间执行
    }

    private def tryRegisterMaster(): JFuture[_] = {
      registerMasterThreadPool.submit(new Runnable {
        override def run(): Unit = try {
          if (registered.get) { // 如果已经注册则返回
            return
          }
          logInfo("Connecting to site master " + siteMasterRpcAddress.toSparkURL + "...")
          val smasterRef = rpcEnv.setupEndpointRefByURI(siteMasterUrl)
          smasterRef.send(RegisterSiteApplication(siteAppDescription, self))
        } catch {
          case ie: InterruptedException => // Cancelled
          case NonFatal(e) => logWarning(
            s"Failed to connect to site master $siteMasterRpcAddress", e)
        }
      })
    }

    private def markDead(reason: String): Unit = {
      if (!alreadyDead.get()) {
        listener.dead(reason)
        alreadyDead.set(true)
      }
    }

    private def markDisconnected(): Unit = {
      if (!alreadyDisconnected) {
        listener.disconnected()
        alreadyDisconnected = true
      }
    }

    /**
     * Send a message to the current site master. If we have not yet registered successfully
     * with any master, the message will be dropped.
     */
    private def sendToSiteMaster(message: Any): Unit = {
      siteMaster match {
        case Some(masterRef) => masterRef.send(message)
        case None => logWarning(s"Drop $message because has not yet connected to site master")
      }
    }

  }

  def stop(): Unit = {
    if (endpoint != null) {
      try {
        val timeout = RpcUtils.askRpcTimeout(conf)
        timeout.awaitResult(endpoint.get.ask[Boolean](StopSiteAppClient))
      } catch {
        case e: TimeoutException =>
          logInfo("Stop request to Site Master timed out; it may already be shut down.")
      }
      endpoint.set(null)
    }
  }

  // ==== 接收来自StandaloneSchedulerBackend的调用请求 ====
  // 会向自身发送消息, 最终再发送给Master

  /**
   * Request executors from the Master by specifying the total number desired,
   * including existing pending and running executors.
   * 从master请求指定数目的executor
   *
   * @return whether the request is acknowledged.
   */
  def requestTotalExecutors(requestedTotal: Int): Future[Boolean] = {
    if (endpoint.get != null && siteAppId.get != null) {
      endpoint.get.ask[Boolean](RequestExecutors(siteAppId.get, requestedTotal))
    } else {
      logWarning("Attempted to request executors before driver fully initialized.")
      Future.successful(false)
    }
  }

  /**
   * Kill the given list of executors through the Master.
   * @return whether the kill request is acknowledged.
   */
  def killExecutors(executorIds: Seq[String]): Future[Boolean] = {
    if (endpoint.get != null && siteAppId.get != null) {
      endpoint.get.ask[Boolean](KillExecutors(siteAppId.get, executorIds))
    } else {
      logWarning("Attempted to kill executors before driver fully initialized.")
      Future.successful(false)
    }
  }

}
