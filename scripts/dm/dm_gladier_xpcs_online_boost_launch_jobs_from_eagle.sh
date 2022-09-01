EAGLE_ENDPOINT=74defd5b-5f61-42fc-bcc4-834c9f376a4f:/XPCSDATA
DM_GLOBUS_LOGIN_FILE=/home/dm/etc/dm.globus-cli.sh
DM_SETUP_FILE=/home/dm/etc/dm.setup.sh

inputDir=$1
filter=$2
qmapFile=$3
experiment=$4
atype=$5

if [ -z $inputDir ] || [ -z $filter ] || [ -z $qmapFile ] || [ -z $experiment ] || [ -z $atype ]; then
    echo "Usage: $0 directory filter qmap experiment atype"
    exit 1
fi

#running as dmadmin this will give globus client credentials
#running as 8idiuser will already have credentials 
source $DM_GLOBUS_LOGIN_FILE 2> /dev/null
source $DM_SETUP_FILE

filelist=(`globus ls $EAGLE_ENDPOINT/$inputDir --filter $filter -r | grep -e ".*\(\.h5\|\.imm\|\.bin\)"`)
for file in "${filelist[@]}"; do
    baseDir=`echo "$file" | cut -d "/" -f1`
    rawFile=`globus ls $EAGLE_ENDPOINT$inputDir$baseDir -r | grep .hdf`
    dm-start-processing-job --workflow-name xpcs8-02-gladier-boost-dev filePath:$inputDir$file qmapFile:$qmapFile atype:$atype experimentName:$experiment rawFile:$rawFile
done

