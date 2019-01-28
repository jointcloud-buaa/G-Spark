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

import org.apache.spark.deploy.sitemaster.SiteDriverDescription

private[globalmaster] class SiteDriverDesc(
  val id: String,
  val application: ApplicationInfo,
  val master: SiteMasterInfo,
  val cores: Int,
  val memory: Int) {
  var state = SiteDriverState.LAUNCHING

  def copyState(sdriverDesc: SiteDriverDescription): Unit = {
    state = sdriverDesc.state
  }

  def fullId: String = application.id + "/" + id

  override def equals(obj: Any): Boolean = {
    obj match {
      case info: SiteDriverDesc =>
        fullId == info.fullId &&
          master.id == info.master.id &&
          cores == info.cores &&
          memory == info.memory
      case _ => false
    }
  }

  override def toString: String = fullId

  override def hashCode(): Int = toString.hashCode
}
