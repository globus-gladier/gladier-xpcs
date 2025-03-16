#!/bin/bash


USAGE_TXT="Usage: permissions.sh workflow_setup_file experiment"

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh}
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup script: $TIME_DIFF seconds"

EXPERIMENT_NAME=$2
if [[ "$#" -lt 2 || "$EXPERIMENT_NAME" == "" ]]; then
    echo "ERROR: Experiment name must be provided."
    echo $USAGE_TXT
    exit 1
fi

RESULT_PATH=${3:-analysis/}

START_TIME=$(date +%s.%N)
dm-restore-permissions --relative-path $RESULT_PATH --experiment $EXPERIMENT_NAME
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time restoring permissions: $TIME_DIFF seconds"
