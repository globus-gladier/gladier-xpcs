#!/bin/sh

ORTHROS_NFS_ROOT="\/net\/wolf"
CLUTCH_DATA_ROOT="\/data\/xpcs8"
DM_SETUP_FILE=/home/dm/etc/dm.setup.sh

inputFile=$1
if [ ! -f $inputFile ]; then
    echo "$inputFile does not exist or it is not a file"
    exit 1
fi

#get globus group id
experimentName=$2
source $DM_SETUP_FILE
#make sure a group exists
createGroup=`dm-create-globus-group --experiment=$experimentName`
getGroup=`dm-get-globus-group --experiment=$experimentName --display-keys id`
#remove 'id='
globusID=`cut -c 4- <<< $getGroup`

inputHdf5File=`basename $inputFile`
inputDir=`dirname $inputFile`
rawFile=`ls -c1 $inputDir/*.{imm,bin} | head -1`
rawFile=`basename $rawFile`

clusterDataDir=`echo $inputDir | sed "s/$ORTHROS_NFS_ROOT//"`
userDataDir=`dirname $clusterDataDir`
cycleDataDir=`dirname $userDataDir`
cycleDataDir=`echo $cycleDataDir | sed "s/$CLUTCH_DATA_ROOT//"`
qmapDir=`echo $CLUTCH_DATA_ROOT/partitionMapLibrary$cycleDataDir | tr -d '\'`

sgeJobName=`basename $clusterDataDir`

echo "Input HDF5 File: $inputHdf5File"
echo "SGE Job Name: $sgeJobName"
echo "Raw Data File: $rawFile"
echo "Cluster Data Directory: $clusterDataDir"
echo "QMap Directory: $qmapDir"
echo "Globus Group ID: $globusID"
