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
package org.apache.spark.scheduler

import java.nio.ByteBuffer

import org.apache.spark.util.SerializableBuffer

private[spark] class StageDescription(
  val stageId: Long,
  val sdriverId: String,
  // TODO-lzp: 考虑下index??
  _serializedStage: ByteBuffer
) extends Serializable {

  private val buffer = new SerializableBuffer(_serializedStage)
  def serializedStage: ByteBuffer = buffer.value

}
