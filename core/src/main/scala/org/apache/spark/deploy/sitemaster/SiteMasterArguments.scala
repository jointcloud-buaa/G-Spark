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

import scala.annotation.tailrec

import org.apache.spark.SparkConf
import org.apache.spark.util.{IntParam, Utils}

private[sitemaster] class SiteMasterArguments(args: Array[String], conf: SparkConf) {
  var host = Utils.localHostName()
  var port = 7088
  var webUiPort = 8090
  var cores = Utils.inferDefaultCores()
  var memory = Utils.inferDefaultMemory()
  var gmasters: Array[String] = _
  var workDir: String = _
  var propertiesFile: String = _


  if (System.getenv("SPARK_SITE_MASTER_HOST") != null) {
    host = System.getenv("SPARK_SITE_MASTER_HOST")
  }
  if (System.getenv("SPARK_SITE_MASTER_PORT") != null) {  // for rpc port
    port = System.getenv("SPARK_SITE_MASTER_PORT").toInt
  }
  if (System.getenv("SPARK_SITE_MASTER_WEBUI_PORT") != null) {  // for webUi port
    webUiPort = System.getenv("SPARK_SITE_MASTER_WEBUI_PORT").toInt
  }
  if (System.getenv("SPARK_SITE_MASTER_CORES") != null) {  // for cores
    cores = System.getenv("SPARK_SITE_MASTER_CORES").toInt
  }
  if (conf.getenv("SPARK_SITE_MASTER_MEMORY") != null) {  // for memory
    memory = Utils.memoryStringToMb(conf.getenv("SPARK_SITE_MASTER_MEMORY"))
  }
  if (System.getenv("SPARK_SITE_MASTER_DIR") != null) {  // for masterDir
    workDir = System.getenv("SPARK_SITE_MASTER_DIR")
  }

  parse(args.toList)

  // This mutates the SparkConf, so all accesses to it must be made after this line
  propertiesFile = Utils.loadDefaultSparkProperties(conf, propertiesFile)

  if (conf.contains("spark.siteMaster.ui.port")) {
    webUiPort = conf.get("spark.siteMaster.ui.port").toInt
  }

  @tailrec
  private def parse(args: List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--momory" | "-m") :: IntParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)

    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case "--properties-file" :: value :: tail =>
      propertiesFile = value
      parse(tail)

    case "--help" :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (gmasters != null) {
        printUsageAndExit(1)
      }
      gmasters = Utils.parseStandaloneMasterUrls(value)
      parse(tail)

    case Nil =>
      if (gmasters == null) printUsageAndExit(1)

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  private def printUsageAndExit(exitCode: Int) {
    // scalastyle:off println
    System.err.println(
      "Usage: SiteMaster [options] <globalMasters>\n" +
        "\n" +
        "Global master must be a URL of the form spark://hostname:port\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST, --host HOST   Hostname to listen on\n" +
        "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
        "  -c CORES, --cores CORES  Number of cores to use\n" +
        "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
        "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
        "  --webui-port PORT      Port for web UI (default: 8080)\n" +
        "  --properties-file FILE Path to a custom Spark properties file.\n" +
        "                         Default is conf/spark-defaults.conf.")
    // scalastyle:on println
    System.exit(exitCode)
  }

  def checkSiteMasterMemory(): Unit = {
    if (memory <= 0) {
      val message = "Memory is below 1MB, or missing a M/G at the end of the memory specification?"
      throw new IllegalStateException(message)
    }
  }
}
