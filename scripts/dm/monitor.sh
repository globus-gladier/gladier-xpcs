#!/bin/bash

# This script is responsible for setting up the client credentials
# and environment before calling the script which fetches the
# Globus flow status

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh}
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup file: $TIME_DIFF seconds"

flowID=$2
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
python $DM_WORKFLOWS_DIR/scripts/get_status.py --run_id $flowID
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time getting globus flow status: $TIME_DIFF seconds"
