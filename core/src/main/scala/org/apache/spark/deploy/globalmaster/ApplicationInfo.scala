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

package org.apache.spark.deploy.globalmaster

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.deploy.ApplicationDescription
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] class ApplicationInfo(
    val startTime: Long,
    val id: String,
    val desc: ApplicationDescription,
    val submitDate: Date,
    val driver: RpcEndpointRef,
    defaultCores: Int)
  extends Serializable {

  @transient var state: ApplicationState.Value = _
  @transient var siteDrivers: mutable.HashMap[String, SiteDriverDesc] = _
  // just for webui
  @transient var removedSiteDrivers: ArrayBuffer[SiteDriverDesc] = _
  @transient var endTime: Long = _
  @transient var appSource: ApplicationSource = _

  // A cap on the number of executors this application can have at any given time.
  // By default, this is infinite. Only after the first allocation request is issued by the
  // application will this be set to a finite value. This is used for dynamic allocation.
//  @transient private[master] var executorLimit: Int = _


  @transient private var nextSiteDriverId: Int = _

  private def createDateFormat = new SimpleDateFormat("yyyyMMddHHmmss", Locale.US)

  init()

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    state = ApplicationState.WAITING
    siteDrivers = new mutable.HashMap[String, SiteDriverDesc]
    endTime = -1L
    nextSiteDriverId = 0
    appSource = new ApplicationSource(this)
    removedSiteDrivers = new ArrayBuffer[SiteDriverDesc]
//    executorLimit = desc.initialExecutorLimit.getOrElse(Integer.MAX_VALUE)
  }

  private def newSiteDriverId(sdriverId: Option[String]): String = {
    sdriverId match {
      case Some(id) =>
        val pat = """site-driver-(\d+)""".r
        val pat(num) = id
        nextSiteDriverId = math.max(nextSiteDriverId, num.toInt + 1)
        "site-driver-%04d".format(nextSiteDriverId)
      case None =>
        val id = nextSiteDriverId
        nextSiteDriverId += 1
        "site-driver-%04d".format(nextSiteDriverId)
    }
  }

  private[globalmaster] def addSiteDriver(
    master: SiteMasterInfo,
    cores: Int,
    sdId: Option[String] = None): SiteDriverDesc = {
    val sdriver = new SiteDriverDesc(
      newSiteDriverId(sdId), this, master, cores, desc.memoryPerSiteDriverMB
    )
    siteDrivers(sdriver.id) = sdriver
    sdriver
  }

  private[globalmaster] def removeSiteDriver(sd: SiteDriverDesc): Unit = {
    if (siteDrivers.contains(sd.id)) {
      removedSiteDrivers += siteDrivers(sd.id)
      siteDrivers -= sd.id
    }
  }

  private val requestedCores = desc.maxCores.getOrElse(defaultCores)

  private var _retryCount = 0

  private[globalmaster] def retryCount = _retryCount

  private[globalmaster] def incrementRetryCount() = {
    _retryCount += 1
    _retryCount
  }

  private[globalmaster] def resetRetryCount() = _retryCount = 0

  private[globalmaster] def markFinished(endState: ApplicationState.Value) {
    state = endState
    endTime = System.currentTimeMillis()
  }

  private[globalmaster] def isFinished: Boolean = {
    state != ApplicationState.WAITING && state != ApplicationState.RUNNING
  }

  /**
   * Return the limit on the number of executors this application can have.
   * For testing only.
   */
//  private[deploy] def getExecutorLimit: Int = executorLimit

  def duration: Long = {
    if (endTime != -1) {
      endTime - startTime
    } else {
      System.currentTimeMillis() - startTime
    }
  }
}
