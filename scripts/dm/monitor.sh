#!/bin/bash

# This script is responsible for setting up the client credentials
# and environment before calling the script which fetches the
# Globus flow status

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh}
source $WORKFLOW_SETUP_FILE

flowID=$2
source $CONDA_PATH
conda activate $CONDA_ENV
python $DM_WORKFLOWS_DIR/scripts/dm/checkStatus.py --flowID $flowID