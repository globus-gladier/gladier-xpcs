# Workflows must be dictionaries

# Workflow keys
#   name (required)
#   owner (required)
#   stages (required, must be dictionary of at least length 1)
#   description (optional)
#   id (assigned by the DB, cannot be modified)

# Stage map keys can be anything; they will get sorted, and stages will
# get executed in the sorted order

# Stage Keys
#   command (required, may use $variable strings that would get their
#     values at runtime, via <key>:<value> arguments)
#   execOutFormat (optional, to be parsed for variables like $jobId )
#   workingDir (optional, default .)
#   dataDir (optional, default $workingDir)
#   monitorCmd (optional, if exists, will execute monitoring in a timer)
#   parallelExec (optional, default True; this flag is relevant only
#     if we iterate over files via $filePath variable)
#   outputVariableRegexList (optional, list of regular expressions
#     that define output variables that may be used in
#     subsequent workflow stages)
#   filePath, filePathList, filePathPattern, or fileQueryDict (optional)

# If job spec contains filePath, filePathList, filePathPattern or
# fileQueryDict, files will be resolved into filePathList
#
# If $filePath is in the command argument list, filePathList is
# iterated over each file for a particular stage

# Variables that get filled automatically
#   $username : DM username
#   $execHost : execution host
#   $id : job id
#   $startTime : procesing start time
#   $endTime : procesing end time

{
    'owner': '8idiuser', 
    'stages': {
            '01-Staging' : {'command': '/home/beams/8IDIUSER/DM_Workflows/xpcs8/start-xpcs8-remote-automate-01.sh $filePath $fileDataDir $xpcsGroupName $sgeQueueName', 'outputVariableRegexList' : ['Cluster Data Directory: (?P<clusterDataDir>.*)', 'Cluster Results Directory: (?P<clusterResultsDir>.*)', 'Cluster JobLogs Directory: (?P<clusterJobLogsDir>.*)', 'Cluster JobErrorLogs Directory: (?P<clusterJobErrorLogsDir>.*)', 'Relative Data Directory: (?P<relativeDataDir>.*)', 'Input HDF5 File: (?P<inputHdf5File>.*)', 'Output HDF5 File: (?P<outputHdf5File>.*)', 'IMM File: (?P<immFile>.*)', 'SGE Job Name: (?P<sgeJobName>.*)', 'SGE Queue Name: (?P<sgeQueueName>.*)', 'File Data Directory: (?P<fileDataDir>.*)', 'User Data Directory: (?P<userDataDir>.*)', 'Cycle Data Directory: (?P<cycleDataDir>.*)', 'ORTHROS RESULTS DIR: (?P<ORTHROS_RESULTS_DIR>.*)', 'ORTHROS RESULTS LOGS RELATIVE DIR: (?P<ORTHROS_RESULTS_LOGS_RELATIVE_DIR>.*)', 'HADOOP FUSE DIR: (?P<HADOOP_FUSE_DIR>.*)', 'ID8I DATA ROOT: (?P<ID8I_DATA_ROOT>.*)', 'ID8I DATA ROOT NET: (?P<ID8I_DATA_ROOT_NET>.*)', 'ID8I GRIDFTP SERVER: (?P<ID8I_GRIDFTP_SERVER>.*)', 'CLUSTER GRIDFTP SERVER: (?P<CLUSTER_GRIDFTP_SERVER>.*)', 'HADOOP GRIDFTP SERVER: (?P<HADOOP_GRIDFTP_SERVER>.*)', 'XPCS Group Name: (?P<xpcsGroupName>.*)', 'ORTHROS DATA ROOT NFS: (?P<ORTHROS_DATA_ROOT_NFS>.*)', 'XPCS JOB FOLDER: (?P<XPCS_JOB_FOLDER>.*)', 'SOURCE ENDPOINT UUID: (?P<SOURCE_ENDPOINT_UUID>.*)', 'SOURCE ENDPOINT DATA ROOT: (?P<SOURCE_ENDPOINT_DATA_ROOT>.*)', 'ALCF Cluster Data Directory: (?P<ALCFclusterDataDir>.*)', 'ALCF Cluster Results Directory: (?P<ALCFclusterResultsDir>.*)']},
            '03-Automate' : {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_xpcs.py --endpoint $SOURCE_ENDPOINT_UUID --input $clusterResultsDir/$outputHdf5File --imm $clusterDataDir/$immFile"', 'outputVariableRegexList' : ['(?P<AutomateId>.*)'],},
            '04-MonitorAutomate' : {'command': '/bin/echo Automate ID: $AutomateId'},
            '05-Automate_Transfer1': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step Transfer1 --walltime 120"'},
            '06-Automate_XPCS': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step ExecCorr --walltime 900"'},
            '07-Automate_CopyBack': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step Transfer2 --walltime 60"'},
     },
    'description': 'XPCS8-03 Workflow Automate', 
    'name': 'xpcs8-03-automate'
}
