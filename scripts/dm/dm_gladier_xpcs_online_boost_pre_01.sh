#!/bin/sh

ORTHROS_NFS_ROOT="\/net\/wolf\/data\/xpcs8"
EAGLE_DATA_ROOT=/XPCSDATA #/lus/eagle/projects/XPCS-DATA-DYS/XPCSDATA
DM_SETUP_FILE=/home/dm/etc/dm.setup.sh

inputFile=$1
if [ -z $inputFile ]; then
    echo "Full path to input file must be provided as an argument"
    exit 1
fi
if [ ! -f $inputFile ]; then
    echo "Input file $inputFile does not exist or it is not a file"
fi

#get globus group id
experimentName=$2
if [ -z $experimentName ]; then
    echo "Name of DM experiment must be provided as an argument"
    exit 1
fi
source $DM_SETUP_FILE
#make sure a group exists
createGroup=`dm-create-globus-group --experiment=$experimentName`
getGroup=`dm-get-globus-group --experiment=$experimentName --display-keys id`
#remove 'id='
globusID=`cut -c 4- <<< $getGroup`

inputHdf5File=`basename $inputFile`
inputDir=`dirname $inputFile`
clusterDataDir=`echo $inputDir | sed "s/$ORTHROS_NFS_ROOT//"`
userDataDir=`dirname $clusterDataDir`
cycleDataDir=`dirname $userDataDir`
cycleDataDir=`basename $cycleDataDir`
sgeJobName=`basename $clusterDataDir`

qmapName=$3
if [ -z $qmapName ]; then
    echo "Name of qmap file must be provided as an argument"
    exit 1
fi
#suppress tr warning about backslash
qmapDir=`echo $EAGLE_DATA_ROOT/partitionMapLibrary/$cycleDataDir | tr -d '\' 2> /dev/null`
qmapFile=$qmapDir/$qmapName
userInputRawFile=$4

if [ ! -z $userInputRawFile ]; then
    rawFile=$userInputRawFile

else   
    # Fetch any .imm, .bin, or .h5. Redirect errors to null
    rawFile=`ls -c1 $inputDir/*.{imm,bin,h5} 2> /dev/null | head -1`
    rawFile=`basename $rawFile 2> /dev/null` 
    if [ -z $rawFile ]; then
        echo "Raw data file not found for input file $inputFile. Expected .imm, .bin, or .h5 raw data file in input directory."
    fi
    if [ ! -f $inputDir/$rawFile ]; then
        echo "Data file $inputDir/$rawFile does not exist or it is not a file"
    fi
    clusterDataDir=$EAGLE_DATA_ROOT$clusterDataDir
    ORTHROS_NFS_ROOT=`echo $ORTHROS_NFS_ROOT | tr -d '\' 2> /dev/null` 
    if [ ! -f $ORTHROS_NFS_ROOT/partitionMapLibrary/$cycleDataDir/$qmapName ]; then
        echo "Qmap file $qmapFile does not exist or it is not a file"
    fi
fi


echo "Input HDF5 File: $inputHdf5File"
echo "SGE Job Name: $sgeJobName"
echo "Raw Data File: $rawFile"
echo "Cluster Data Directory: $clusterDataDir"
echo "QMap File: $qmapFile"
echo "Globus Group ID: $globusID"
