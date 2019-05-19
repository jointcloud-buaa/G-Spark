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

// 带宽这里比我想像的还要复杂些，即(n1,n2)理解为n1到n2的带宽，即n1的发送带宽
// 而(n2,n1)则理解为n2到n1的带宽，即n2的发送带宽
// TODO-lzp: 目前关于网络测量数据的表示有好几种方式，感觉应该统一下：
//           1, idxMap映射, bws二维数组， latencies二维数组
//           2, 主机到主机的映射, Map[String, Map[String, (Long, Double)]], 有两种：
//              外层的key是表示发射主机还是接收主机
case class NetworkDistState(
  idxMap: Map[String, Int],
  bws: Array[Array[Long]],  // Bps, 字节每秒
  latencies: Array[Array[Double]]  // ms, 毫秒, TODO-lzp: 似乎有点大
) {
  // 主机到主机的带宽, 外层key为接收主机
  def hostToHostMap: Map[String, Map[String, (Long, Double)]] = idxMap.map { case (h1, idx1) =>
    (h1, idxMap.filterKeys(_ != h1).map { case (h2, idx2) =>
      // 应该是h2到h1的带宽和延迟
        (h2, (bws(idx2)(idx1), latencies(idx2)(idx1)))
      }.toMap)
  }.toMap

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
      Array.fill[Double](len, len)(0)
    )
  }

  // 常量意味着, 带宽: 节点到自己是无限大, 到别人是恒速; 延迟: 到自己为0, 到别人是恒量
  def const(idxMap: Map[String, Int], bw: Long, latency: Double): NetworkDistState = {
    val len = idxMap.size
    NetworkDistState(
      idxMap,
      Array.tabulate[Long](len, len)((r, c) => if (r == c) Long.MaxValue else bw),
      Array.tabulate[Double](len, len)((r, c) => if (r == c) 0 else latency)
    )
  }

  // 模拟Wlan环境，bws为每个节点设置的输出带宽，而latencies则为每个节点设置的延迟
  def mockWlan(
    idxMap: Map[String, Int],
    bws: Array[Long],
    latencies: Array[Double],
    lanBw: Long = Long.MaxValue): NetworkDistState = {
    val len = idxMap.size
    require(len == bws.length && len == latencies.length, "the args length does not match")
    NetworkDistState(
      idxMap,
      Array.tabulate[Long](len, len)((r, c) =>
        if (r == c) lanBw
        else bws(r)  // a -> b的带宽为a的输出带宽
      ),
      Array.tabulate[Double](len, len)((r, c) =>
        if (r == c) 0
        else latencies(r) + latencies(c)  // a -> b的延迟为a的延迟加上b的延迟
      )
    )
  }

  def apply(
    data: Map[String, Map[String, (Long, Double)]],  // 外层key为发送主机
    lanBw: Long = Long.MaxValue): NetworkDistState = {
    val idxMap = data.keys.zipWithIndex.toMap
    val len = data.size
    val bws = Array.tabulate[Long](len, len)((r, c) => if (r == c) lanBw else 0)
    val latencies = Array.fill[Double](len, len)(0)
    data.map { case (sendH, m) =>
        m.map { case (recvH, v) =>
            bws(idxMap(sendH))(idxMap(recvH)) = v._1
            latencies(idxMap(sendH))(idxMap(recvH)) = v._2
        }
    }
    NetworkDistState(idxMap, bws, latencies)
  }
}
