# Uses globus cli to get a list of files before starting processing jobs 
# for gladier boost workflow in the case when data is already on eagle

EAGLE_ENDPOINT=74defd5b-5f61-42fc-bcc4-834c9f376a4f
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


filelist=(`globus ls $EAGLE_ENDPOINT:$inputDir --filter $filter`)
for baseDir in "${filelist[@]}"; do
    filename=`globus ls $EAGLE_ENDPOINT:$inputDir/$datasetDir | grep -e ".*\(\.h5\|\.imm\|\.bin\)"`
    file="$datasetDir$filename"
    metadata=`globus ls $EAGLE_ENDPOINT:$inputDir$baseDir -r | grep .hdf`
    file=`echo "$file" | cut -d "/" -f2`
    dm-start-processing-job --workflow-name xpcs8-02-gladier-boost filePath:$inputDir$baseDir/$metadata qmapFile:$qmapFile atype:$atype experimentName:$experiment rawFile:$file
done

