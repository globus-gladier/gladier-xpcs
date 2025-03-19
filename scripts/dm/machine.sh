#!/bin/bash 

USAGE_TXT="Usage: machine.sh setup_file machine_name" 

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh} 
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup script: $TIME_DIFF seconds"

ANALYSIS_MACHINE=${2:-$DEFAULT_ANALYSIS_MACHINE} 

# Verify the machine given by the user is one that is set up for this 
# analysis. It should have Voyager mounted, DM LDAP configured, ssh 
# access from the DM VM by the beamline user account and 
# the analysis software installed. 

for machine in "${ANALYSIS_MACHINES[@]}"; do     
    if [[ "$ANALYSIS_MACHINE" == "$machine" ]]; then       
        echo "Analysis Machine: $ANALYSIS_MACHINE"       
        exit 0     
    fi 
done 

echo -n "ERROR: Analysis machine must be one of: " 
for machine in "${ANALYSIS_MACHINES[@]}"; do     
    echo -n "$machine " 
done 
echo 
echo "Default analysis machine is $DEFAULT_ANALYSIS_MACHINE" 
echo $USAGE_TXT 
exit 1
