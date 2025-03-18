#!/bin/bash


USAGE_TXT="Usage: permissions.sh workflow_setup_file experiment"

WORKFLOW_SETUP_FILE=${1:-/home/dm/etc/dm.workflow_setup.sh}
START_TIME=$(date +%s.%N)
source $WORKFLOW_SETUP_FILE
END_TIME=$(date +%s.%N)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time sourcing setup file: $TIME_DIFF seconds"

EXPERIMENT_NAME=$2
if [[ "$#" -lt 2 || "$EXPERIMENT_NAME" == "" ]]; then
    echo "ERROR: Experiment name must be provided."
    echo $USAGE_TXT
    exit 1
fi

RESULT_PATH=${3:-analysis/}
IFS='/' read -ra SUBDIRS <<< "$RESULT_PATH"
# Check if any of the subdirectories were created during
# analysis, and if so, restore permissions from there
RESTORE_PATH=""
for SUBDIR in "${SUBDIRS[@]}"; do
    if [[ -n "$SUBDIR" ]]; then
        RESTORE_PATH+="/$SUBDIR"
        if [[ "$RESTORE_PATH" == *"analysis/"* ]]; then
            OWNER=$(stat -c '%U' "$RESTORE_PATH" 2>/dev/null) 
            if [ "$OWNER" != "dmadmin" ]; then
                RELATIVE_RESTORE_PATH="analysis${RESTORE_PATH#*analysis}"
                START_TIME=$(date +%s.%N)
                dm-restore-permissions --relative-path $RELATIVE_RESTORE_PATH --experiment $EXPERIMENT_NAME
                if [ $? != 0 ]; then
                    echo "ERROR: Unable to restore permissions for experiment $EXPERIMENT_NAME path $RELATIVE_RESTORE_PATH" 
                    exit 1
                fi
                END_TIME=$(date +%s.%N)
                TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
                TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
                echo "Time restoring permissions: $TIME_DIFF seconds"
                echo "Restored permissions for experiment $EXPERIMENT_NAME path $RELATIVE_RESTORE_PATH"
                exit 0
            fi
        fi
    fi
done

# New files in an existing directory
RESTORE_PATH=$RESULT_PATH
RELATIVE_RESTORE_PATH="analysis${RESTORE_PATH#*analysis}"
START_TIME=$(date +%s)
dm-restore-permissions --relative-path $RELATIVE_RESTORE_PATH --experiment $EXPERIMENT_NAME
if [ $? != 0 ]; then
    echo "ERROR: Unable to restore permissions for experiment $EXPERIMENT_NAME path $RELATIVE_RESTORE_PATH" 
    exit 1
fi
END_TIME=$(date +%s)
TIME_DIFF=$(awk "BEGIN {print $END_TIME - $START_TIME}")
TIME_DIFF=$(printf "%.2f" "$TIME_DIFF")
echo "Time restoring permissions: $TIME_DIFF seconds"
echo "Restored permissions for experiment $EXPERIMENT_NAME path $RELATIVE_RESTORE_PATH"