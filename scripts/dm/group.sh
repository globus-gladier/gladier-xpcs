#!/bin/bash

# This script is responsible for fetching
# or creating the globus group which will
# be used to assign permission to data published
# in a portal

USAGE_TXT="Usage: group.sh setup_file experiment_name"

WORKFLOW_SETUP_FILE=${1:-/home/dm/workflows/dm.workflow_setup.sh}
source $WORKFLOW_SETUP_FILE

#get globus group id
EXPERIMENT_NAME=$2

#make sure a group exists
createGroup=`dm-create-globus-group --experiment $EXPERIMENT_NAME`
getGroup=`dm-get-globus-group --experiment $EXPERIMENT_NAME --display-keys id`
if [ $? != 0 ]; then
    echo "ERROR: Unable to create or get Globus group for experiment $EXPERIMENT_NAME"
    exit 1
fi
#make sure dmadmin can admin group
dm-add-globus-group-members --experiment $EXPERIMENT_NAME --admins dmadmin@globusid.org 
#remove 'id='
globusID=`cut -c 4- <<< $getGroup`
echo "Globus Group: $globusID"