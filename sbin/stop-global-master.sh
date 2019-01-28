#!/usr/bin/env bash

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

. "${SPARK_HOME}/sbin/spark-config.sh"

"${SPARK_HOME}/sbin"/spark-daemon.sh stop org.apache.spark.deploy.globalmaster.GlobalMaster 1
