#!/bin/sh

ORTHROS_USER_ACCOUNT=8idiuser@orthros
BEAMLINE_USER_ACCOUNT=8idiuser@talc
ORTHROS_DATA_ROOT=/data/xpcs8
ORTHROS_PROCESSING_ROOT=/data/xpcs8
ORTHROS_RESULTS_DIR=cluster_results_ALCF
ORTHROS_RESULTS_LOGS_RELATIVE_DIR=Job_Logs
ORTHROS_RESULTS_ERRORLOGS_RELATIVE_DIR=JobError_Logs
ID8I_DATA_ROOT=/home/8-id-i
ID8I_DATA_ROOT_NET=/export/8-id-i
DEFAULT_SGE_QUEUE=xpcs8new.q
HADOOP_FUSE_DIR=/hdata/user/xpcs8
CLUSTER_GRIDFTP_SERVER=wolf08:2811
ID8I_GRIDFTP_SERVER=s8id-dmdtn:2811
HADOOP_GRIDFTP_SERVER=hpcs08:2811
DEFAULT_XPCS_Group=/xpcs
ORTHROS_NFS_ROOT=/net/wolf
ORTHROS_DATA_ROOT_NFS=${ORTHROS_NFS_ROOT}/data/xpcs8
XPCS_JOB_FOLDER=XPCS_Job_Files_Temp
APSDATA=9c9cb97e-de86-11e6-9d15-22000a1e3b52
CLUTCH=b0e921df-6d04-11e5-ba46-22000b92c6ec
CLUTCHDMZ=fdc7e74a-fa78-11e8-9342-0e3d676669f4
PETRELXPCS=e55b4eab-6d04-11e5-ba46-22000b92c6ec
SOURCE_ENDPOINT_UUID=$CLUTCHDMZ
APSDATA_DATA_ROOT=/gdata/dm/8IDI
CLUTCH_DATA_ROOT=/data/xpcs8
CLUTCHDMZ_DATA_ROOT=/data/xpcs8
PETRELXPCS_DATA_ROOT=/XPCSDATA
SOURCE_ENDPOINT_DATA_ROOT=$CLUTCHDMZ_DATA_ROOT
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

xpcsGroupName=$3
if [ -z "$xpcsGroupName" ]; then
    xpcsGroupName=$DEFAULT_XPCS_Group
fi

sgeQueueName=$4
if [ -z "$sgeQueueName" ]; then
    sgeQueueName=$DEFAULT_SGE_QUEUE
fi

#get globus group id
experimentName=$5
source $DM_SETUP_FILE
#make sure a group exists
createGroup=`dm-create-globus-group --experiment=$experimentName`
getGroup=`dm-get-globus-group --experiment=$experimentName --display-keys id`
#remove 'id='
globusID=`cut -c 4- <<< $getGroup`

inputHdf5File=`basename $inputFile`
outputHdf5File=$inputHdf5File

immFile=`ls -c1 $inputDir/*.{imm,bin} | head -1`
immFile=`basename $immFile`

ALCF_RESULTS_DIR=$ORTHROS_DATA_ROOT_NFS/$cycleDataDir/$userDataDir/$ORTHROS_RESULTS_DIR
sgeJobName=$fileDataDir
ALCFclusterDataDir=$SOURCE_ENDPOINT_DATA_ROOT/$relativeDataDir
ALCFclusterResultsDir=`realpath -m $ALCFclusterDataDir/../$ORTHROS_RESULTS_DIR`
clusterDataDir=$ORTHROS_DATA_ROOT/$relativeDataDir
clusterResultsDir=`realpath -m $clusterDataDir/../$ORTHROS_RESULTS_DIR`
clusterJobLogsDir=$clusterResultsDir/$ORTHROS_RESULTS_LOGS_RELATIVE_DIR
clusterJobErrorLogsDir=$clusterResultsDir/$ORTHROS_RESULTS_ERRORLOGS_RELATIVE_DIR
qmapDir=$CLUTCH_DATA_ROOT/partitionMapLibrary/$cycleDataDir

ssh $ORTHROS_USER_ACCOUNT "mkdir -p $clusterResultsDir; mkdir -p $clusterDataDir; mkdir -p $clusterJobLogsDir; mkdir -p $clusterJobErrorLogsDir; mkdir -p $ALCF_RESULTS_DIR; chmod 777 $ALCF_RESULTS_DIR" || exit 1

echo "Input Directory: $inputDir"
echo "Relative Data Directory: $relativeDataDir"
echo "File Data Directory: $fileDataDir"
echo "User Data Directory: $userDataDir"
echo "Cycle Data Directory: $cycleDataDir"
echo "Input HDF5 File: $inputHdf5File"
echo "Output HDF5 File: $outputHdf5File"
echo "IMM Full File Path: $fullImmFilePath"
echo "IMM File: $immFile"
echo "Cluster Data Directory: $clusterDataDir"
echo "Cluster Results Directory: $clusterResultsDir"
echo "Cluster JobLogs Directory: $clusterJobLogsDir"
echo "Cluster JobErrorLogs Directory: $clusterJobErrorLogsDir"
echo "SGE Queue Name: $sgeQueueName"
echo "SGE Job Name: $sgeJobName"
echo "ORTHROS RESULTS DIR: $ORTHROS_RESULTS_DIR"
echo "ORTHROS RESULTS LOGS RELATIVE DIR: $ORTHROS_RESULTS_LOGS_RELATIVE_DIR"
echo "ORTHROS RESULTS ERROR LOGS RELATIVE DIR: $ORTHROS_RESULTS_ERRORLOGS_RELATIVE_DIR"
echo "HADOOP FUSE DIR: $HADOOP_FUSE_DIR"
echo "CLUSTER GRIDFTP SERVER: $CLUSTER_GRIDFTP_SERVER"
echo "HADOOP GRIDFTP SERVER: $HADOOP_GRIDFTP_SERVER"
echo "ID8I GRIDFTP SERVER: $ID8I_GRIDFTP_SERVER"
echo "ID8I DATA ROOT: $ID8I_DATA_ROOT"
echo "ID8I DATA ROOT NET: $ID8I_DATA_ROOT_NET"
echo "XPCS Group Name: $xpcsGroupName"
echo "ORTHROS DATA ROOT NFS: $ORTHROS_DATA_ROOT_NFS"
echo "XPCS Group Name: $xpcsGroupName"
echo "SOURCE ENDPOINT UUID: $SOURCE_ENDPOINT_UUID"
echo "SOURCE ENDPOINT DATA ROOT: $SOURCE_ENDPOINT_DATA_ROOT"
echo "ALCF Cluster Results Directory: $ALCF_RESULTS_DIR"
echo "ALCF Cluster Data Directory: $ALCFclusterDataDir"
echo "QMap Directory: $qmapDir"
echo "Globus Group ID: $globusID"