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
package org.apache.spark.scheduler.cluster

import scala.collection.Map

case class NetworkDistState(
  idxMap: Map[String, Int],
  bws: Array[Array[Long]],  // Bps, 字节每秒
  latencies: Array[Array[Int]]  // ms, 毫秒
) {
  override def toString: String =
    s"""NetworkDistState:
       |  idxMap: $idxMap,
       |  bws: ${bws.map(_.mkString(",")).mkString("||")},
       |  latencies: ${latencies.map(_.mkString(",")).mkString("||")}
     """.stripMargin
}

object NetworkDistState {
  // 空意味着节点到节点的网速是均匀的  TODO-lzp: 不确定是不是应该无限大, 还是给个有限的值
  def empty(idxMap: Map[String, Int]): NetworkDistState = {
    val len = idxMap.size
    NetworkDistState(
      idxMap,
      Array.fill[Long](len, len)(Long.MaxValue),
      Array.fill[Int](len, len)(0)
    )
  }

  // 常量意味着, 带宽: 节点到自己是无限大, 到别人是恒速; 延迟: 到自己为0, 到别人是恒量
  def const(idxMap: Map[String, Int], bw: Long, latency: Int): NetworkDistState = {
    val len = idxMap.size
    NetworkDistState(
      idxMap,
      Array.tabulate[Long](len, len)((r, c) => if (r == c) Long.MaxValue else bw),
      Array.tabulate[Int](len, len)((r, c) => if (r == c) 0 else latency)
    )
  }
}
