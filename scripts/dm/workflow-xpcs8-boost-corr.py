##dm-upsert-workflow --py-spec workflow-xpcs8-02-gladier-boost.py
##dm-start-processing-job \
# --workflow-name xpcs8-02-gladier-boost \
# filePath:/net/wolf/data/xpcs8/2022-1/comm202202/E010_Ceramic_Lq1_000C_att03_001/E010_Ceramic_Lq1_000C_att03_001_0001-1000.hdf \
# qmapFile:comm202202_qmap_Ceramic_Lq0_S270_D54.h5 \
# atype:Multitau
# experimentName:test-xpcs-boost-workflow

{
    'owner': '8idiuser',
    'stages': {
        '01-ANALYSIS-MACHINE' : {
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/machine.sh ' + \
                       '/home/dm/etc/dm.workflow_setup.sh ' + \
                       '\"$analysisMachine\"',
            'outputVariableRegexList' : [
                'Analysis Machine: (?P<analysisMachine>.*)',
            ]
        },
        '02-PARSE-ARGS' : {
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/parse.sh ' + \
                '\"$experimentName\" ' + \
                '\"$filePath\" ' + \
                '\"$qmap\" ' + \
                '\"$smooth\" ' + \
                '\"$gpuID\" ' + \
                '\"$beginFrame\" ' + \
                '\"$endFrame\" ' + \
                '\"$strideFrame\" ' + \
                '\"$avgFrame\" ' + \
                '\"$type\" ' + \
                '\"$dq\" ' + \
                '\"$verbose\" ' + \
                '\"$saveG2\" ' + \
                '\"$overwrite\" ' + \
                '\"$outputDir\" ',
            'outputVariableRegexList' : [
                'Metadata File: (?P<metadata>.*)',
                'Result File: (?P<resultFile>.*)',
                'Boost Corr Arguments: (?P<boostCorrArgs>.*)',
            ]
        },
        '03-LOCAL' : {
            'runIf': '"$analysisMachine" != "polaris"',
            'command': 'ssh $analysisMachine \"boost_corr_bin $boostCorrArgs\"'
        },
        '04-GROUP' : {
            'runIf': '"$analysisMachine" == "polaris"',
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/group.sh ' + \
                       '/home/dm/etc/dm.workflow_setup.sh ' + \
                       '\"$experimentName\"',
            'outputVariableRegexList' : [
                'Globus Group: (?P<globusGroup>.*)',
            ]
        },
        '05-POLARIS' : {
            'runIf': '"$analysisMachine" == "polaris"',
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/gladier.sh ' + \
                       '/home/dm/etc/dm.workflow_setup.sh ' + \
                       '$experimentName $globusGroup $metadata $boostCorrArgs',
            'outputVariableRegexList' : [
                'Flow Action ID: (?P<flowActionID>.*)',
                'URL: (?P<url>.*)',
                'Status: (?P<gladierStatus>.*)'
            ]
        },
        '06-MONITOR' : {
            'runIf': '"$analysisMachine" == "polaris"',
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/monitor.sh ' + \
                        '/home/dm/etc/dm.workflow_setup.sh ' + \
                       '$flowActionID',
        },
        '07-PERMISSIONS' : {
            'command': 'sh /home/dm/workflows/xpcs8/gladier-xpcs/scripts/dm/permissions.sh ' + \
                '/home/dm/etc/dm.workflow_setup.sh ' + \
                '$experimentName $resultFile',
        },
        '08-DONE' : {
            'command': '/bin/echo Job done.'
        },
    },
    'description': 'XPCS8 Polaris Development Workflow for post-APSU.\n' + \
        'Keyword Arguments:\n' + \
        '\texperimentName - Name of the experiment, which corresponds to file paths on Voyager.\n' + \
        '\tfilePath - Name of the raw data file located in the experiment data directory.\n' + \
        '\tqmap - Name of the qmap file located in the experiment data directory.\n' + \
        '\tsmooth (optional) - smooth method to be used in Twotime correlation. default: sqmap\n' + \
        '\tgpuID (optional) - choose which GPU to use. default: -1 (CPU)\n' + \
        '\tbeginFrame (optional) - specifies which frame to begin with for the correlation. default: 1\n' + \
        '\tendFrame (optional) - specifies the last frame used for the correlation. default: -1\n' + \
        '\tstrideFrame (optional) - defines the stride. default: 1\n' + \
        '\tavgFrame (optional) - defines the number of frames to be averaged before the correlation. default: 1\n' + \
        '\ttype (optional) - analysis type Multitau, Twotime, or Both. default: Multitau\n' + \
        '\tdq (optional) - a string that selects the dq list, eg. \'1, 2, 5-7\' selects [1,2,5,6,7]. default: all\n' + \
        '\tverbose (optional) default: False\n' + \
        '\tsaveG2 (optional) - save G2, IP, and IF to file. default: False\n' + \
        '\toverwrite (optional) - overwrite the existing result file.  default: False\n',
    'name': 'xpcs8-boost-corr',
    'userAccount': '8idiuser'
}