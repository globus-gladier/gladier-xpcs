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
            'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/Automate_ryan/reprocessing/',

            # General info info
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'flags': '',
            'qmap_source_endpoint': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
            'qmap_source_path': '/XPCSDATA/Automate/qmap/sanat201903_qmap_S270_D54_lin.h5',
            'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
            'qmap_file': '/projects/APSDataAnalysis/Automate_ryan/reprocessing/sanat201903_qmap_S270_D54_lin.h5',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',
            'reprocessing_suffix': '_ryan_reprocessing_test',

            # Execution Environment (For Ryan only).
            'funcx_endpoint_non_compute': '6c4323f4-a062-4551-a883-146a352a43f5',
            'funcx_endpoint_compute': '9f84f41e-dfb6-4633-97be-b46901e9384c',

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
