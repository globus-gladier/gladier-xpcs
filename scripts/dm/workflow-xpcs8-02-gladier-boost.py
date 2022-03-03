##dm-upsert-workflow --py-spec workflow-xpcs8-02-gpu-gladier.py

{
    'owner': '8idiuser',
    'name': 'xpcs8-02-gladier-boost',
    'description': 'XPCS8-02 workflow to run online boost processing using gladier',
    'stages': {
        '01-Staging' : {
            'command': '/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/dm/dm_gladier_xpcs_online_boost_pre_01.sh \
                $filePath $qmapFile $experimentName',
            'outputVariableRegexList' : [
                'Cluster Data Directory: (?P<clusterDataDir>.*)',
                'Input HDF5 File: (?P<inputHdf5File>.*)',
                'Raw Data File: (?P<rawFile>.*)',
                'QMap Directory: (?P<qmapDir>.*)',
                'Globus Group ID: (?P<globusID>.*)'
            ],
        },
        '02-Automate' : {
            'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/xpcs_online_boost_client.py \
                    --hdf $clusterDataDir/$inputHdf5File \
                    --raw $clusterDataDir/$rawFile \
                    --qmap $qmapDir/$qmapFile \
                    --gpu_flag $gpuFlag \
                    --group $globusID \
                    --atype $atype \
                    --batch_size $batchSize \
                    -d $deployment"',
            'outputVariableRegexList' : [
                'run_id : (?P<AutomateId>.*)'
            ],
        },
        '03-MonitorAutomate' : {'command': '/bin/echo https://app.globus.org/runs/$AutomateId'},
        '04-Automate_TransferOut': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step TransferFromClutchToTheta --timeout 300 --gpu"'},
        '05-Automate_Corr': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step XpcsBoostCorr --timeout 7200 --gpu"'},
        '06-Automate_Plots': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step MakeCorrPlots --timeout 7200 --gpu"'},
        '07-Automate_TransferBack': {'command': 'ssh 8idiuser@talc "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step GatherXpcsMetadata --timeout 120 --gpu"'},
	}
}
