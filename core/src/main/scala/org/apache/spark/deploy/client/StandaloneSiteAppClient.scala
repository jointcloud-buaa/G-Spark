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

import java.util.concurrent.{Future => JFuture, ScheduledFuture => JScheduledFuture, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

import scala.util.control.NonFatal

import org.apache.spark.SparkConf
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.sitemaster.{SiteAppDescription, SiteMaster}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.ThreadUtils

private[spark] class StandaloneSiteAppClient(
  rpcEnv: RpcEnv,
  siteMasterUrls: Array[String],
  siteAppDescription: SiteAppDescription,
  listener: StandaloneSiteAppClientListener,
  conf: SparkConf
) extends Logging {

  private val siteMasterRpcAddresses = siteMasterUrls.map(RpcAddress.fromSparkURL)

  private val REGISTRATION_TIMEOUT_SECONDS = 20
  private val REGISTRATION_RETRIES = 3

  private val endpoint = new AtomicReference[RpcEndpointRef]  // appClient
  private val siteAppId = new AtomicReference[String]
  private val registered = new AtomicBoolean(false)

  private val registerMasterThreadPool = ThreadUtils.newDaemonCachedThreadPool(
    "site-appclient-register-sitemaster-threadpool",
    siteMasterRpcAddresses.length // Make sure we can register with all masters at the same time
  )

  // A scheduled executor for scheduling the registration actions
  private val registrationRetryThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor(
    "site-appclient-registration-retry-thread")

  private class SiteClientEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint with Logging {

    private var siteMaster: Option[RpcEndpointRef] = None

    // To avoid calling listener.disconnected() multiple times
    private var alreadyDisconnected = false
    // To avoid calling listener.dead() multiple times
    private val alreadyDead = new AtomicBoolean(false)

    private val registerSiteMasterFutures = new AtomicReference[Array[JFuture[_]]]
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

    }

    override def receive: PartialFunction[Any, Unit] = {
      case RegisteredSiteApplication(siteAppId_, smRef) =>
        siteAppId.set(siteAppId_)
        registered.set(true)
        siteMaster = Some(smRef)
        listener.connected(siteAppId.get())

      case SiteMasterChanged(smRef, smWebUiUrl) =>
        logInfo("Site Master has changed, new site master is at " + smRef.address.toSparkURL)
        siteMaster = Some(smRef)
        alreadyDisconnected = false
        smRef.send(SiteMasterChangeAcknowledged(siteAppId.get))

    }

    // TODO-lzp: wait to fill code
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case _ =>
    }

    override def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {

    }

    override def onDisconnected(remoteAddress: RpcAddress): Unit = {

    }

    private def registerWithSiteMaster(nthRetry: Int): Unit = {
      registerSiteMasterFutures.set(tryRegisterAllMasters())
      registrationRetryTimer.set(registrationRetryThread.schedule(new Runnable {
        override def run(): Unit = {
          if (registered.get) {  // 如果已经注册成功
            registerSiteMasterFutures.get.foreach(_.cancel(true))
            registerMasterThreadPool.shutdownNow()
          } else if (nthRetry >= REGISTRATION_RETRIES) {  // 超过注册次数
            markDead("All masters are unresponsive! Giving up.")
          } else {  // 因为是延时执行的, 此时仍然没有成功, 则取消之前的结果, 重试
            registerSiteMasterFutures.get.foreach(_.cancel(true))
            registerWithSiteMaster(nthRetry + 1)
          }
        }
      }, REGISTRATION_TIMEOUT_SECONDS, TimeUnit.SECONDS))  // 延时多长时间执行
    }

    private def tryRegisterAllMasters(): Array[JFuture[_]] = {
      for (smasterAddr <- siteMasterRpcAddresses) yield {
        registerMasterThreadPool.submit(new Runnable {
          override def run(): Unit = try {
            if (registered.get) {  // 如果已经注册则返回
              return
            }
            logInfo("Connecting to site master " + smasterAddr.toSparkURL + "...")
            val smasterRef = rpcEnv.setupEndpointRef(smasterAddr, SiteMaster.ENDPOINT_NAME)
            smasterRef.send(RegisterSiteApplication(siteAppDescription, self))
          } catch {
            case ie: InterruptedException => // Cancelled
            case NonFatal(e) => logWarning(s"Failed to connect to site master $smasterAddr", e)
          }
        })
      }
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

  }

}
