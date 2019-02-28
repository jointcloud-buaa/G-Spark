#!/usr/bin/env bash

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [[ $# -lt 1 ]] || [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-cluster.sh <global-master>"
  exit 1
fi

GLOBAL_MASTER=$1
shift

# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"

# Start Master
"${SPARK_HOME}/sbin"/start-site-master.sh $GLOBAL_MASTER

# Start Workers
"${SPARK_HOME}/sbin"/start-slaves.sh
