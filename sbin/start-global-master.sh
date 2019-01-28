#!/usr/bin/env bash

if [ -z "${SPARK_HOME}" ]; then
  export SPARK_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

# NOTE: This exact class name is matched downstream by SparkSubmit.
# Any changes need to be reflected there.
CLASS="org.apache.spark.deploy.globalmaster.GlobalMaster"

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  echo "Usage: ./sbin/start-global-master.sh [options]"
  pattern="Usage:"
  pattern+="\|Using Spark's default log4j profile:"
  pattern+="\|Registered signal handlers for"

  "${SPARK_HOME}"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
  exit 1
fi

ORIGINAL_ARGS="$@"

. "${SPARK_HOME}/sbin/spark-config.sh"

. "${SPARK_HOME}/bin/load-spark-env.sh"

if [ "$SPARK_GLOBAL_MASTER_PORT" = "" ]; then
  SPARK_GLOBAL_MASTER_PORT=7077
fi

if [ "$SPARK_GLOBAL_MASTER_HOST" = "" ]; then
  case `uname` in
      (SunOS)
	  SPARK_GLOBAL_MASTER_HOST="`/usr/sbin/check-hostname | awk '{print $NF}'`"
	  ;;
      (*)
	  SPARK_GLOBAL_MASTER_HOST="`hostname -f`"
	  ;;
  esac
fi

if [ "$SPARK_GLOBAL_MASTER_WEBUI_PORT" = "" ]; then
  SPARK_GLOBAL_MASTER_WEBUI_PORT=8080
fi

"${SPARK_HOME}/sbin"/spark-daemon.sh start $CLASS 1 \
  --host $SPARK_GLOBAL_MASTER_HOST --port $SPARK_GLOBAL_MASTER_PORT --webui-port $SPARK_GLOBAL_MASTER_WEBUI_PORT \
  $ORIGINAL_ARGS
