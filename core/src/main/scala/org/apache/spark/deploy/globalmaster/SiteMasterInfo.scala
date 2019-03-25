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

import scala.collection.mutable

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

private[spark] class SiteMasterInfo(
  val id: String,
  val host: String,
  val port: Int,
  val cores: Int,
  val memory: Int,
  val endpoint: RpcEndpointRef,
  val webUiAddress: String
) extends Serializable {

  Utils.checkHost(host, "Expected hostname")
  assert(port > 0)

  @transient var globalDrivers: mutable.HashMap[String, GlobalDriverInfo] = _
  @transient var siteDrivers: mutable.HashMap[String, SiteDriverDesc] = _

  @transient var state: SiteMasterOutState.Value = _
  @transient var coresUsed: Int = _
  @transient var memoryUsed: Int = _

  @transient var lastHeartbeat: Long = _

  init()

  def coresFree: Int = cores - coresUsed
  def memoryFree: Int = memory - memoryUsed

  private def readObject(in: java.io.ObjectInputStream): Unit = Utils.tryOrIOException {
    in.defaultReadObject()
    init()
  }

  private def init() {
    globalDrivers = new mutable.HashMap
    siteDrivers = new mutable.HashMap

    state = SiteMasterOutState.ALIVE
    coresUsed = 0
    memoryUsed = 0
    lastHeartbeat = System.currentTimeMillis()
  }

  def hostPort: String = {
    assert (port > 0)
    host + ":" + port
  }

  def setState(state: SiteMasterOutState.Value): Unit = {
    this.state = state
  }

  def addGlobalDriver(gdriver: GlobalDriverInfo): Unit = {
    globalDrivers(gdriver.id) = gdriver
    memoryUsed += gdriver.desc.mem
    coresUsed += gdriver.desc.cores
  }

  def removeGlobalDriver(gdriver: GlobalDriverInfo): Unit = {
    globalDrivers -= gdriver.id
    memoryUsed -= gdriver.desc.mem
    coresUsed -= gdriver.desc.cores
  }

  def addSiteDriver(sdDesc: SiteDriverDesc): Unit = {
    // the application id is the key, because the SiteMaster just has one siteDriver
    // for a application at anytime.
    siteDrivers(sdDesc.application.id) = sdDesc
    coresUsed += sdDesc.cores
    memoryUsed += sdDesc.memory
  }

  def removeSiteDriver(sdriver: SiteDriverDesc): Unit = {
    if (siteDrivers.contains(sdriver.application.id)) {
      siteDrivers -= sdriver.application.id
      coresUsed -= sdriver.cores
      memoryUsed -= sdriver.memory
    }
  }

  def hasSiteDriver(appId: String): Boolean = siteDrivers.contains(appId)

  def isAlive(): Boolean = this.state == SiteMasterOutState.ALIVE
}
