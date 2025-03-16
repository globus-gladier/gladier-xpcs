#!/bin/bash 

# This script is responsible for launching the gladier client

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh}
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup script: $TIME_DIFF seconds"

EXPERIMENT=$2
GROUP=$3
METADATA_FILE_PATH=$4
#get all args after the 4th
shift 4
BOOST_CORR_ARGS=$@

START_TIME=$(date +%s.%N)
source $CONDA_PATH
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing conda script: $TIME_DIFF seconds"

START_TIME=$(date +%s.%N)
conda activate $CONDA_ENV
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time activating conda env: $TIME_DIFF seconds"

START_TIME=$(date +%s.%N)
python $DM_WORKFLOWS_DIR/scripts/xpcs_online_boost_client.py --experiment $EXPERIMENT --group $GROUP --hdf $METADATA_FILE_PATH $BOOST_CORR_ARGS
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time submitting globus flow: $TIME_DIFF seconds"
