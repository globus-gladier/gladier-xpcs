##dm-upsert-workflow --py-spec workflow-xpcs8-02-gladier-boost.py
##dm-start-processing-job \
# --workflow-name xpcs8-02-gladier-boost \
# filePath:/net/wolf/data/xpcs8/2022-1/comm202202/E010_Ceramic_Lq1_000C_att03_001/E010_Ceramic_Lq1_000C_att03_001_0001-1000.hdf \
# qmapFile:comm202202_qmap_Ceramic_Lq0_S270_D54.h5 \
# atype:Multitau 
# experimentName:test-xpcs-boost-workflow

{
    'owner': '8idiuser',
    'name': 'xpcs8-02-gladier-boost',
    'description': 'XPCS8-02 workflow to run online boost processing using gladier',
    'stages': {
        '00-name'  : {'command': 'echo xpcs8-02-gladier-boost', 'outputVariableRegexList' : ['(?P<name>.*)']},
        '01-Staging' : {
            'command': '/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/dm/dm_gladier_xpcs_online_boost_pre_01.sh \
                $filePath $experimentName $qmapFile $rawFile',
            'outputVariableRegexList' : [
                'Cluster Data Directory: (?P<clusterDataDir>.*)',
                'SGE Job Name: (?P<sgeJobName>.*)', 
                'Input HDF5 File: (?P<inputHdf5File>.*)',
                'Raw Data File: (?P<rawFile>.*)',
                'QMap File: (?P<qmapFile>.*)',
                'Globus Group ID: (?P<globusID>.*)'
            ],
        },
        '02-Automate' : {
            'command': 'ssh 8idiuser@s8ididm "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/xpcs_online_boost_client.py \
                    --hdf $clusterDataDir/$inputHdf5File \
                    --raw $clusterDataDir/$rawFile \
                    --qmap $qmapFile \
                    --atype $atype \
                    --group $globusID \
                    --verbose \
                    -d nick-polaris-gpu"',
            'outputVariableRegexList' : [
                'run_id : (?P<AutomateId>.*)'
            ],
        },
        '03-MonitorAutomate' : {'command': '/bin/echo https://app.globus.org/runs/$AutomateId'},
        '04-Automate_TransferOut': {'command': 'ssh 8idiuser@s8ididm "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step TransferFromClutchToTheta --timeout 900 --gpu"'},
        '05-Automate_Corr': {'command': 'ssh 8idiuser@s8ididm "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step XpcsBoostCorr --timeout 7200 --gpu"'},
        '06-Automate_Plots': {'command': 'ssh 8idiuser@s8ididm "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step MakeCorrPlots --timeout 7200 --gpu"'},
        '07-Automate_TransferBack': {'command': 'ssh 8idiuser@s8ididm "/home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/gladier-xpcs/scripts/get_status.py --run_id $AutomateId --step GatherXpcsMetadata --timeout 600 --gpu"'},
	}
}
