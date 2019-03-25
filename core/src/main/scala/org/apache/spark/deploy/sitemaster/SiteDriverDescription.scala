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

import org.apache.spark.deploy.globalmaster.SiteDriverState

private[deploy] class SiteDriverDescription(
  val appId: String,
  val sdriverId: String,
  val cores: Int,
  val state: SiteDriverState.Value
) extends Serializable {

  override def toString: String =
    "SiteDriverState(appId=%s, sdriverId=%s, cores=%d, state=%s)".format(
      appId, sdriverId, cores, state)
}
