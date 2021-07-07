{
    'owner': '8idiuser', 
    'stages': {
            '01-Staging' : {
                'command': '/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/dm/dm_gladier_xpcs_pre_01.sh \
                    $filePath $fileDataDir $xpcsGroupName $sgeQueueName', 
                'outputVariableRegexList' : [
                    'Cluster Data Directory: (?P<clusterDataDir>.*)', 
                    'Cluster Results Directory: (?P<clusterResultsDir>.*)', 
                    'Cluster JobLogs Directory: (?P<clusterJobLogsDir>.*)', 
                    'Cluster JobErrorLogs Directory: (?P<clusterJobErrorLogsDir>.*)', 
                    'Relative Data Directory: (?P<relativeDataDir>.*)', 
                    'Input HDF5 File: (?P<inputHdf5File>.*)', 
                    'Output HDF5 File: (?P<outputHdf5File>.*)', 
                    'IMM File: (?P<immFile>.*)', 
                    'SGE Job Name: (?P<sgeJobName>.*)', 
                    'SGE Queue Name: (?P<sgeQueueName>.*)', 
                    'File Data Directory: (?P<fileDataDir>.*)', 
                    'User Data Directory: (?P<userDataDir>.*)', 
                    'Cycle Data Directory: (?P<cycleDataDir>.*)', 
                    'ORTHROS RESULTS DIR: (?P<ORTHROS_RESULTS_DIR>.*)', 
                    'ORTHROS RESULTS LOGS RELATIVE DIR: (?P<ORTHROS_RESULTS_LOGS_RELATIVE_DIR>.*)', 
                    'HADOOP FUSE DIR: (?P<HADOOP_FUSE_DIR>.*)', 'ID8I DATA ROOT: (?P<ID8I_DATA_ROOT>.*)', 
                    'ID8I DATA ROOT NET: (?P<ID8I_DATA_ROOT_NET>.*)', 
                    'ID8I GRIDFTP SERVER: (?P<ID8I_GRIDFTP_SERVER>.*)', 
                    'CLUSTER GRIDFTP SERVER: (?P<CLUSTER_GRIDFTP_SERVER>.*)', 
                    'HADOOP GRIDFTP SERVER: (?P<HADOOP_GRIDFTP_SERVER>.*)', 
                    'XPCS Group Name: (?P<xpcsGroupName>.*)', 
                    'ORTHROS DATA ROOT NFS: (?P<ORTHROS_DATA_ROOT_NFS>.*)', 
                    'XPCS JOB FOLDER: (?P<XPCS_JOB_FOLDER>.*)', 'SOURCE ENDPOINT UUID: (?P<SOURCE_ENDPOINT_UUID>.*)', 
                    'SOURCE ENDPOINT DATA ROOT: (?P<SOURCE_ENDPOINT_DATA_ROOT>.*)', 
                    'ALCF Cluster Data Directory: (?P<ALCFclusterDataDir>.*)', 
                    'ALCF Cluster Results Directory: (?P<ALCFclusterResultsDir>.*)'
                    ],
                },
            '03-Automate' : {
                'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py \
                    --hdf $clusterResultsDir/$outputHdf5File \
                    --imm $clusterDataDir/$immFile"',
                 #   --group None"' , ##activate when group can be passed 
                'outputVariableRegexList' : [
                    '(?P<AutomateId>.*)'
                ],
            },
            # '04-MonitorAutomate' : {'command': '/bin/echo Automate ID: $AutomateId'},
            # '05-Automate_Transfer1': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step Transfer1 --walltime 120"'},
            # '06-Automate_XPCS': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step ExecCorr --walltime 900"'},
            # '07-Automate_CopyBack': {'command': 'ssh 8idiuser@talc "/home/beams/8IDIUSER/DM_Workflows/xpcs8/automate/automate_wait.py --task_id $AutomateId --step Transfer2 --walltime 60"'},
     },
    'description': 'XPCS8-01 Gladier Workflow', 
    'name': 'xpcs8-01-gladier'
}