#!/usr/bin/env bash

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# Load the Spark configuration
. "${SPARK_HOME}/sbin/spark-config.sh"

# Stop the slaves, then the master
"${SPARK_HOME}/sbin"/stop-slaves.sh
"${SPARK_HOME}/sbin"/stop-site-master.sh

if [ "$1" == "--wait" ]
then
  printf "Waiting for workers to shut down..."
  while true
  do
    running=`${SPARK_HOME}/sbin/slaves.sh ps -ef | grep -v grep | grep deploy.worker.Worker`
    if [ -z "$running" ]
    then
      printf "\nAll workers successfully shut down.\n"
      break
    else
      printf "."
      sleep 10
    fi
  done
fi
