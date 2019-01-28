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

import java.net.URI

import org.apache.spark.deploy.Command

// TODO-lzp
private[spark] case class SiteAppDescription(
  name: String,
  maxCores: Option[Int],
  memoryPerExecutorMB: Int,
  command: Command,  // to start executor
  appUiUrl: String,
  eventLogDir: Option[URI] = None,
  eventLogCodec: Option[Int] = None,
  coresPerExecutor: Option[Int] = None,
  user: String = System.getProperty("user.name", "<unknown>")) {

  override def toString: String = "SiteAppDescription(" + name + ")"
}
