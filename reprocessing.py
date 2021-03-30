from gladier.client import GladierClient
# This is a HACK to enable glaider logging
import gladier.tests
from pprint import pprint


class XPCSReprocessing(GladierClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        # 'gladier_tools.manifest.tools.ManifestTransfer',
        # 'gladier_tools.manifest.tools.ManifestToFuncXTasks',
        'manifest_reprocessing.XPCSManifestTool',
        'gladier_tools.xpcs.ApplyQmap',
        'gladier_tools.xpcs.EigenCorr',
        'gladier_tools.xpcs.MakeCorrPlots',
        'gladier_tools.xpcs.CustomPilot',
    ]

    flow_definition = 'manifest_reprocessing.XPCSManifestTool'
    # flow_definition = 'gladier_tools.xpcs.ApplyQmap'
    # flow_definition = 'gladier_tools.xpcs.EigenCorr'
    # flow_definition = 'gladier_tools.xpcs.MakeCorrPlots'
    # Warning: Renames dataset to "mydataset_qmap"!
    # flow_definition = 'gladier_tools.xpcs.CustomPilot'


if __name__ == '__main__':
    flow_input = {
        'input': {
            # Manifest input files, and destination for where to process (Anyone can use the manifest below)
            'manifest_id': '8b5c50ff-838d-4072-ad6c-ce9d142d6b04',
            'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/Automate/reprocessing/',

            # General info info
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'flags': '',
            'qmap_file': 'sanat201903_qmap_S270_D54_lin.h5',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',
            'reprocessing_suffix': '_nick_reprocessing_test',

            # Execution Environment (For Nick only).
            'funcx_endpoint_non_compute': '0a162a09-8bd9-4dd9-b046-0bfd635d38a7',
            'funcx_endpoint_compute': '37e6099f-e9e7-4817-ac68-4afcd78d8221',

            # Needed for running on gladier_tools directly, otherwise not used
            'proc_dir': '/lus/theta-fs0/projects/APSDataAnalysis/nick/xpcs',
            'hdf_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
            'imm_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm',
        }
    }
    re_cli = XPCSReprocessing()
    # pprint(re_cli.get_input())
    corr_flow = re_cli.start_flow(flow_input=flow_input)
    re_cli.progress(corr_flow['action_id'])
    pprint(re_cli.get_status(corr_flow['action_id']))
