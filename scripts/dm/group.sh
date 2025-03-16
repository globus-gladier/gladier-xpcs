#!/bin/bash

# This script is responsible for fetching
# or creating the globus group which will
# be used to assign permission to data published
# in a portal

USAGE_TXT="Usage: group.sh setup_file experiment_name"

WORKFLOW_SETUP_FILE=${1:-/home/dm/workflows/dm.workflow_setup.sh}
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup file: $TIME_DIFF seconds"

#get globus group id
EXPERIMENT_NAME=$2

#make sure a group exists
START_TIME=$(date +%s.%N)
createGroup=`dm-create-globus-group --experiment $EXPERIMENT_NAME`
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time creating globus group: $TIME_DIFF seconds"

START_TIME=$(date +%s.%N)
getGroup=`dm-get-globus-group --experiment $EXPERIMENT_NAME --display-keys id`
if [ $? != 0 ]; then
    echo "ERROR: Unable to create or get Globus group for experiment $EXPERIMENT_NAME"
    exit 1
fi
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time verifying globus group creation: $TIME_DIFF seconds"

#make sure dmadmin can admin group
START_TIME=$(date +%s.%N)
dm-add-globus-group-members --experiment $EXPERIMENT_NAME --admins dmadmin@globusid.org 
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time adding dmadmin to globus group: $TIME_DIFF seconds"

#remove 'id='
globusID=`cut -c 4- <<< $getGroup`
echo "Globus Group: $globusID"
