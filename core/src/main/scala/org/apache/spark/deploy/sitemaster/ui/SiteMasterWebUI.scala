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

package org.apache.spark.deploy.sitemaster.ui

import scala.collection.mutable

import org.eclipse.jetty.servlet.ServletContextHandler

import org.apache.spark.deploy.sitemaster.SiteMaster
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{SparkUI, WebUI}
import org.apache.spark.ui.JettyUtils._

private[sitemaster] class SiteMasterWebUI(
                                         val siteMaster: SiteMaster,
                                         requestedPort: Int)
  extends WebUI(siteMaster.securityMgr, siteMaster.securityMgr.getSSLOptions("standalone"),
    requestedPort, siteMaster.conf, name = "SiteMasterUI")
  with Logging{

  private val proxyHandlers = new mutable.HashMap[String, ServletContextHandler]()

  initialize()

  // TODO-lzp: add some page
  override def initialize(): Unit = {
    attachHandler(createStaticHandler(SiteMasterWebUI.STATIC_RESOURCE_BASE, "/static"))
  }

  def addProxyTargets(id: String, target: String): Unit = {
    var endTarget = target.stripSuffix("/")
    val handler = createProxyHandler("/proxy/" + id, endTarget)
    attachHandler(handler)
    proxyHandlers(id) = handler
  }

  def removeProxyTargets(id: String): Unit = {
    proxyHandlers.remove(id).foreach(detachHandler)
  }
}

private[sitemaster] object SiteMasterWebUI {
  val STATIC_RESOURCE_BASE = SparkUI.STATIC_RESOURCE_DIR
  val DEFAULT_RETAINED_GLOBAL_DRIVERS = 1000
  val DEFAULT_RETAINED_SITE_DRIVERS = 1000
}
