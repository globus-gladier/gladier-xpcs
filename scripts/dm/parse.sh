#!/bin/bash

USAGE_TXT="Usage: parse.sh experiment raw qmap [smooth gpu_id begin_frame end_frame stride_fram avg_frame type dq verbose save_g2 overwrite]"

EXPERIMENT_NAME=$1
if [ "$#" -lt 1 ]; then
    echo "ERROR: Experiment name must be provided."
    echo $USAGE_TXT
    exit 1
fi

RAW_FILE=$2
if [ "$#" -lt 2 ]; then
    echo "ERROR: Raw file name must be provided."
    echo $USAGE_TXT
    exit 1
fi

START_TIME=$(date +%s.%N)
source /home/dm/etc/dm.setup.sh
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup file: $TIME_DIFF seconds"

#get the experiment directory
START_TIME=$(date +%s.%N)
EXPERIMENT_DIRECTORY=`dm-get-experiment --experiment=$EXPERIMENT_NAME --display-keys storageDirectory`
if [ $? != 0 ]; then
    echo "ERROR: Unable to get experiment $EXPERIMENT_NAME."
    echo $USAGE_TXT
    exit 1
fi
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time getting experiment directory: $TIME_DIFF seconds"

#remove 'storageDirectory='
EXPERIMENT_DIRECTORY=`cut -c 18- <<< $EXPERIMENT_DIRECTORY`
#remove trailing space
EXPERIMENT_DIRECTORY=${EXPERIMENT_DIRECTORY::-1}
echo $EXPERIMENT_DIRECTORY
DATASET_NAME=${RAW_FILE%%.*}
RAW_FILE_PATH="$EXPERIMENT_DIRECTORY/data/$DATASET_NAME/$RAW_FILE"
BOOST_CORR_ARGS="-r $RAW_FILE_PATH"

QMAP_FILE=$3
if [ "$#" -lt 3 ]; then
    echo "ERROR: Qmap file name must be provided."
    echo $USAGE_TXT
    exit 1
fi
QMAP_FILE_PATH="$EXPERIMENT_DIRECTORY/data/$QMAP_FILE"
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -q $QMAP_FILE_PATH"

SMOOTH=${4:-sqmap}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -s $SMOOTH"

GPU_ID=${5:-0}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -i $GPU_ID"

BEGIN_FRAME=${6:-0}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -b $BEGIN_FRAME"

END_FRAME=${7:--1}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -e $END_FRAME"

STRIDE_FRAME=${8:-1}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -f $STRIDE_FRAME"

AVG_FRAME=${9:-1}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -a $AVG_FRAME"

TYPE=${10:-Multitau}
if [[ "$TYPE" != "Multitau" && "$TYPE" != "Twotime" && "$TYPE" != "Both" ]]; then
    echo "ERROR: Analysis type must be one of 'Multitau', 'Twotime', or 'Both'"
    echo $USAGE_TXT
    exit 1
fi
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -t $TYPE"

DQ_SELECTION=${11:-all}
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -d \\\"$DQ_SELECTION\\\""

VERBOSE=${12:-True}
if [[ "$VERBOSE" != "False" && "$VERBOSE" != "false" && "$VERBOSE" != "0" ]]; then
    BOOST_CORR_ARGS="$BOOST_CORR_ARGS -v"
fi

SAVE_G2=${13:-False}
if [[ "$SAVE_G2" != "False" && "$SAVE_G2" != "false" && "$SAVE_G2" != "0" ]]; then
    BOOST_CORR_ARGS="$BOOST_CORR_ARGS -G"
fi

OVERWRITE=${14:-False}
if [[ "$OVERWRITE" != "False" && "$OVERWRITE" != "false" && "$OVERWRITE" != "0" ]]; then
    BOOST_CORR_ARGS="$BOOST_CORR_ARGS -w"
fi

OUTPUT_DIR_BASE="$EXPERIMENT_DIRECTORY/analysis/$TYPE"
#optionally user can specify an output dir
USER_OUTPUT_DIR=${15}
OUTPUT_DIR=$OUTPUT_DIR_BASE/$USER_OUTPUT_DIR
BOOST_CORR_ARGS="$BOOST_CORR_ARGS -o $OUTPUT_DIR"

#metadata file is not given to boost corr as an arg
METADATA_FILE_PATH="$EXPERIMENT_DIRECTORY/data/$DATASET_NAME/${DATASET_NAME}_metadata.hdf"
if [[ "$TYPE" == "Twotime" ]]; then
    RESULT_FILE_PATH=$OUTPUT_DIR/${DATASET_NAME}_${TYPE}.hdf
else
    RESULT_FILE_PATH=$OUTPUT_DIR/${DATASET_NAME}_results.hdf
fi

echo "Metadata File: $METADATA_FILE_PATH"
echo "Result File: $RESULT_FILE_PATH"
echo "Boost Corr Arguments: $BOOST_CORR_ARGS"
