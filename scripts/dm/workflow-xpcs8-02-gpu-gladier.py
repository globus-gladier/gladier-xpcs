##dm-upsert-workflow --py-spec workflow-xpcs8-02-gpu-gladier.py

{
    'owner': '8idiuser',
    'name': 'xpcs8-02-gpu-gladier',
    'description': 'XPCS8-02 workflow to run gpu corr processing using gladier',
    'stages': {
        '01-Staging' : {
            'command': '/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/dm/dm_gladier_xpcs_gpu_pre_01.sh \
                $filePath $fileDataDir $xpcsGroupName $sgeQueueName $experimentName',
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
                'ALCF Cluster Results Directory: (?P<ALCFclusterResultsDir>.*)',
                'QMap Directory: (?P<qmapDir>.*)',
                'Globus Group ID: (?P<globusID>.*)'
            ],
        },
        '02-Automate' : {
            'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/xpcs_online_boost_client.py \
                    --hdf $clusterDataDir/$inputHdf5File \
                    --raw $clusterDataDir/$immFile \
                    --qmap $qmapDir/$qmapFile \
                    --gpu_flag 0 \
                    --group $globusID \
                    -d nick-polaris-gpu"',
            'outputVariableRegexList' : [
                'run_id : (?P<AutomateId>.*)'
            ],
        },
        '03-MonitorAutomate' : {'command': '/bin/echo https://app.globus.org/runs/$AutomateId'},
        '04-Automate_TransferOut': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step TransferFromClutchToTheta --timeout 300 --gpu"'},
        '05-Automate_Corr': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step EigenCorrGpu --timeout 7200 --gpu"'},
        '06-Automate_Plots': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step MakeCorrPlots --timeout 7200 --gpu"'},
        '07-Automate_TransferBack': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step GatherXpcsMetadata --timeout 120 --gpu"'},
	}
}
