#!/bin/bash


USAGE_TXT="Usage: permissions.sh workflow_setup_file experiment"

WORKFLOW_SETUP_FILE=${1:-/home/dm/workflows/dm.workflow_setup.sh}
source $WORKFLOW_SETUP_FILE

EXPERIMENT_NAME=$2
if [[ "$#" -lt 2 || "$EXPERIMENT_NAME" == "" ]]; then
    echo "ERROR: Experiment name must be provided."
    echo $USAGE_TXT
    exit 1
fi

dm-restore-permissions --relative-path analysis/ --experiment $EXPERIMENT_NAME