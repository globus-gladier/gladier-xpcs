#!/bin/bash 

# This script is responsible for launching the gladier client

WORKFLOW_SETUP_FILE=${1:-/home/dm/workflows/dm.workflow_setup.sh}
source $WORKFLOW_SETUP_FILE

EXPERIMENT=$2
GROUP=$3
METADATA_FILE_PATH=$4
#get all args after the 4th
shift 4
BOOST_CORR_ARGS=$@

source $CONDA_PATH
conda activate $CONDA_ENV

python $DM_WORKFLOWS_DIR/scripts/xpcs_online_boost_client.py --experiment $EXPERIMENT --group $GROUP --hdf $METADATA_FILE_PATH $BOOST_CORR_ARGS