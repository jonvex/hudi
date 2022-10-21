#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# NOTE: this script runs inside hudi-ci-bundle-validation container
# $WORKDIR/jars/ is supposed to be mounted to a host directory where bundle jars are placed
# TODO: $JAR_COMBINATIONS should have different orders for different jars to detect class loading issues

WORKDIR=/opt/bundle-validation
HIVE_DATA=${WORKDIR}/data/hive
JAR_DATA=${WORKDIR}/data/jars
UTILITIES_DATA=${WORKDIR}/data/utilities

test_spark_bundle () {
    echo "::warning::validate.sh setting up hive sync"
    # put config files in correct place
    cp $HIVE_DATA/spark-defaults.conf $SPARK_HOME/conf/
    cp $HIVE_DATA/hive-site.xml $HIVE_HOME/conf/
    ln -sf $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/hive-site.xml
    cp $DERBY_HOME/lib/derbyclient.jar $SPARK_HOME/jars/

    $DERBY_HOME/bin/startNetworkServer -h 0.0.0.0 &
    $HIVE_HOME/bin/hiveserver2 &
    echo "::warning::validate.sh hive setup complete. Testing"
    $SPARK_HOME/bin/spark-shell --jars $JAR_DATA/spark.jar < $HIVE_DATA/validate.scala
    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh failed hive testing"
        exit 1
    fi
    echo "::warning::validate.sh hive testing succesfull. Cleaning up hive sync"
    # remove config files
    rm -f $SPARK_HOME/jars/derbyclient.jar
    unlink $SPARK_HOME/conf/hive-site.xml
    rm -f $HIVE_HOME/conf/hive-site.xml
    rm -f $SPARK_HOME/conf/spark-defaults.conf
}

make_commands_file () {
    OUTPUT_DIR="/tmp/${OUTPUT_DIR_NAME}/"
    COMMANDS_FILE=$UTILITIES_DATA/commands-${OUTPUT_DIR_NAME}.scala
    echo "val hudiDf = spark.read.format(\"org.apache.hudi\").load(\"${OUTPUT_DIR}\")" > $COMMANDS_FILE
    cat $UTILITIES_DATA/commands.scala >> $COMMANDS_FILE
    rm -rf $OUTPUT_DIR
    echo "::debug::output dir is $OUTPUT_DIR"
}

run_deltastreamer () {
    echo "::warning::validate.sh running deltastreamer"
    echo "::debug::main jar is ${MAIN_JAR}"
    echo "::debug::opt jars is ${OPT_JARS}"
    echo "::debug::output dir is ${OUTPUT_DIR}"
    $SPARK_HOME/bin/spark-submit --driver-memory 8g --executor-memory 8g \
    --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
    $OPT_JARS $MAIN_JAR \
    --props $UTILITIES_DATA/newProps.props \
    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
    --source-class org.apache.hudi.utilities.sources.JsonDFSSource \
    --source-ordering-field ts --table-type MERGE_ON_READ \
    --target-base-path file://${OUTPUT_DIR} \
    --target-table utilities_tbl  --op UPSERT

    if [ "$?" -ne 0 ]; then
        echo "::error::validate.sh deltastreamer failed"
        exit 1
    fi
    echo "::warning::validate.sh done with deltastreamer"
}

test_utilities_bundle () {
    OPT_JARS=""
    if [[ -n $ADDITIONAL_JARS ]]; then
        OPT_JARS="--jars $ADDITIONAL_JARS"
    fi
    run_deltastreamer
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    OUTPUT_SIZE=$(du -s ${OUTPUT_DIR} | awk '{print $1}')
    if [[ -z $OUTPUT_SIZE || "$OUTPUT_SIZE" -lt "$EXPECTED_SIZE" ]]; then
        echo "::error::validate.sh deltastreamer output folder ($OUTPUT_SIZE) is smaller than expected (${EXPECTED_SIZE})" 
        exit 1
    fi
    echo "::debug::output size is $OUTPUT_SIZE, expected $EXPECTED_SIZE"
    IDK_ASFDASF=$(ls -lh ${OUTPUT_DIR})
    echo "::debug::vexler ls -lh is $IDK_ASFDASF"
    IDK_ADFSAREWF=$(du -sh ${OUTPUT_DIR})
    echo "::debug::vexler du -sh is $IDK_ADFSAREWF"
    echo "::warning::validate.sh validating deltastreamer in spark shell"
    SHELL_COMMAND="$SPARK_HOME/bin/spark-shell --jars $ADDITIONAL_JARS $MAIN_JAR $SHELL_ARGS -i $COMMANDS_FILE"
    echo "::debug::this is the shell command: $SHELL_COMMAND"
    LOGFILE="$WORKDIR/submit.log"
    $SHELL_COMMAND > $LOGFILE
    if [ "$?" -ne 0 ]; then
        SHELL_RESULT=$(cat $LOGFILE | grep "Counts don't match")
        echo "::error::validate.sh $SHELL_RESULT"
        exit 1
    fi
    SHELL_RESULT=$(cat $LOGFILE)
    echo "::debug::spark shell output: $SHELL_RESULT"
    echo "::warning::validate.sh done validating deltastreamer in spark shell"
}

test_utilities_bundle_upgrade () {
    mkdir $UTILITIES_DATA/tmpdata
    mv $UTILITIES_DATA/data/batch_2.json $UTILITIES_DATA/tmpdata/
    echo "::debug::data folder contents below"
    ls $UTILITIES_DATA/data
    make_commands_file
    EXPECTED_SIZE=275
    test_utilities_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    MAIN_JAR=$JAR_DATA/utilities.jar
    ADDITIONAL_JARS=""
    EXPECTED_SIZE=550
    mv $UTILITIES_DATA/tmpdata/batch_2.json $UTILITIES_DATA/data/
    echo "::debug::data folder contents below"
    ls $UTILITIES_DATA/data
    test_utilities_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    
}


test_utilities_bundle_downgrade () {
    mkdir $UTILITIES_DATA/tmpdata
    mv $UTILITIES_DATA/data/batch_2.json $UTILITIES_DATA/tmpdata/
    make_commands_file
    EXPECTED_SIZE=275
    MAIN_JAR=$JAR_DATA/utilities.jar
    ADDITIONAL_JARS=""
    test_utilities_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
    MAIN_JAR=$DOWNGRADE_MAIN_JAR
    ADDITIONAL_JARS=$DOWNGRADE_ADDITIONAL_JARS
    EXPECTED_SIZE=550
    mv $UTILITIES_DATA/tmpdata/batch_2.json $UTILITIES_DATA/data/
    test_utilities_bundle
    if [ "$?" -ne 0 ]; then
        exit 1
    fi
}




# test_spark_bundle
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi

SHELL_ARGS=$(cat $UTILITIES_DATA/shell_args)

echo "::warning::validate.sh testing utilities bundle"
MAIN_JAR=$JAR_DATA/utilities.jar
ADDITIONAL_JARS=""
OUTPUT_DIR_NAME=hudi-utilities-test
EXPECTED_SIZE=550
make_commands_file
test_utilities_bundle
if [ "$?" -ne 0 ]; then
    exit 1
fi
echo "::warning::validate.sh done testing utilities bundle"

# echo "::warning::validate.sh testing utilities slim bundle"
# MAIN_JAR=$JAR_DATA/utilities-slim.jar
# ADDITIONAL_JARS=$JAR_DATA/spark.jar
# OUTPUT_DIR_NAME=hudi-utilities-slim-test
# EXPECTED_SIZE=550
# make_commands_file
# test_utilities_bundle
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities slim bundle"


# echo "::warning::validate.sh testing utilities bundle upgrade from 0.11.1"
# MAIN_JAR="${UTILITIES_BUNDLE_0_11_1}"
# ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="upgrade-test_0_11_1"
# test_utilities_bundle_upgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle upgrade from 0.11.1"


# echo "::warning::validate.sh testing utilities bundle upgrade from 0.12.0"
# MAIN_JAR="${UTILITIES_BUNDLE_0_12_0}"
# ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="upgrade-test_0_12_0"
# test_utilities_bundle_upgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle upgrade from 0.12.0"

# echo "::warning::validate.sh testing utilities bundle upgrade from 0.12.1"
# MAIN_JAR="${UTILITIES_BUNDLE_0_12_1}"
# ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="upgrade-test_0_12_1"
# test_utilities_bundle_upgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle upgrade from 0.12.1"


# echo "::warning::validate.sh testing utilities bundle downgrade to 0.11.1"
# DOWNGRADE_MAIN_JAR="${UTILITIES_BUNDLE_0_11_1}"
# DOWNGRADE_ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="downgrade-test_0_11_1"
# test_utilities_bundle_downgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle downgrade to 0.11.1"


# echo "::warning::validate.sh testing utilities bundle downgrade to 0.12.0"
# DOWNGRADE_MAIN_JAR="${UTILITIES_BUNDLE_0_12_0}"
# DOWNGRADE_ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="downgrade-test_0_12_0"
# test_utilities_bundle_downgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle downgrade to 0.12.0"

# echo "::warning::validate.sh testing utilities bundle downgrade to 0.12.1"
# DOWNGRADE_MAIN_JAR="${UTILITIES_BUNDLE_0_12_1}"
# DOWNGRADE_ADDITIONAL_JARS=""
# OUTPUT_DIR_NAME="downgrade-test_0_12_1"
# test_utilities_bundle_downgrade
# if [ "$?" -ne 0 ]; then
#     exit 1
# fi
# echo "::warning::validate.sh done testing utilities bundle downgrade to 0.12.1"

