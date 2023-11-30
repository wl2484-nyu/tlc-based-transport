#!/bin/bash
# Owner: Wan-Yu Lin

#sbt clean
#sbt compile
#sbt package

APP_VER="0.3.0"
SCALA_VER="2.12"
CLASS_NAME="etl.TaxiZones"
JAR_PATH="tlc-based-transport_${SCALA_VER}-${APP_VER}.jar"
SOURCE_PATH="/user/wl2484_nyu_edu/project/data/source/tlc/zones"
CLEAN_OUTPUT_PATH="/user/wl2484_nyu_edu/project/data/clean/tlc/zones"
PROFILE_OUTPUT_PATH="/user/wl2484_nyu_edu/project/data/profile/tlc/zones"

hadoop fs -rm -r ${CLEAN_OUTPUT_PATH}
hadoop fs -rm -r ${PROFILE_OUTPUT_PATH}

spark-submit --deploy-mode cluster \
  --class ${CLASS_NAME} ${JAR_PATH} \
  --source ${SOURCE_PATH} \
  --clean-output ${CLEAN_OUTPUT_PATH} \
  --profile-output ${PROFILE_OUTPUT_PATH}
