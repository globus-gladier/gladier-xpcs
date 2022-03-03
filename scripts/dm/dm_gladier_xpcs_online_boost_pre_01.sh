#!/bin/sh

ORTHROS_DATA_ROOT=/data/xpcs8
ORTHROS_NFS_ROOT=/net/wolf
ORTHROS_DATA_ROOT_NFS=${ORTHROS_NFS_ROOT}/data/xpcs8
CLUTCH_DATA_ROOT=/data/xpcs8
DM_SETUP_FILE=/home/dm/etc/dm.setup.sh

# Input file path: (case where $fileDataDir is not in the inputFile, so it has to be specified as $2)
# /net/wolfa/data/xpcs8/$cycleDataDir/$userDataDir/$ORTHROS_RESULTS_DIR/$fileName
inputFile=$1
if [ ! -f $inputFile ]; then
    echo "$inputFile does not exist or it is not a file"
    exit 1
fi

fileDataDir=$2
if  [ $# -lt 2 ]; then
     echo "DataDir is not specified as the 2nd arg"
     exit 1
fi

inputDir_tmp=`dirname $inputFile`
inputDir=`dirname $inputDir_tmp`/$fileDataDir
cmd="relativeDataDir=\`echo $inputDir | sed 's?$ORTHROS_DATA_ROOT_NFS\/??g'\`"
eval $cmd

userDataDir=`dirname $relativeDataDir`
cycleDataDir=`dirname $userDataDir`
userDataDir=`basename $userDataDir`

#get globus group id
experimentName=$3
source $DM_SETUP_FILE
#make sure a group exists
createGroup=`dm-create-globus-group --experiment=$experimentName`
getGroup=`dm-get-globus-group --experiment=$experimentName --display-keys id`
#remove 'id='
globusID=`cut -c 4- <<< $getGroup`

inputHdf5File=`basename $inputFile`
rawFile=`ls -c1 $inputDir/*.{imm,bin} | head -1`
rawFile=`basename $rawFile`

clusterDataDir=$ORTHROS_DATA_ROOT/$relativeDataDir
qmapDir=$CLUTCH_DATA_ROOT/partitionMapLibrary/$cycleDataDir

echo "Input HDF5 File: $inputHdf5File"
echo "Raw Data File: $rawFile"
echo "Cluster Data Directory: $clusterDataDir"
echo "QMap Directory: $qmapDir"
echo "Globus Group ID: $globusID"
