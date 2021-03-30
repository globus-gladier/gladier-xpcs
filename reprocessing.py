from gladier.client import GladierClient
# This is a HACK to enable glaider logging
import gladier.tests
from pprint import pprint


class XPCSReprocessing(GladierClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.manifest.tools.ManifestTransfer',
        'gladier_tools.manifest.tools.ManifestToFuncXTasks',
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
    nick_theta_login = '0a162a09-8bd9-4dd9-b046-0bfd635d38a7'
    nick_theta_compute = '37e6099f-e9e7-4817-ac68-4afcd78d8221'
    flow_input = {
        'input': {
            'proc_dir': '/lus/theta-fs0/projects/APSDataAnalysis/nick/xpcs',
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'hdf_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
            'imm_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm',
            'flags': '',
            'qmap_file': 'sanat201903_qmap_S270_D54_lin.h5',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',
            # 'output_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',

            # Manifest giblets
            'manifest_to_funcx_tasks_manifest_id': '80cae0bb-fe9c-4f91-ac03-93e1ac550b7e',
            # 'manifest_to_funcx_tasks_callback_funcx_id': '5d071573-2fc8-4e6c-8fa0-8b53a94d07f3',
            # 'manifest_to_reprocessing_task_funcx_id': 'c5d5b3fa-3f9b-4cc4-bbf0-8b855f16de82',
            # 'manifest_to_funcx_tasks_use_dirs': True,
            'funcx_endpoint_non_compute': nick_theta_login,
            'funcx_endpoint_compute': nick_theta_compute,
            'manifest_to_funcx_tasks_funcx_endpoint_compute': nick_theta_compute,
        }
    }
    re_cli = XPCSReprocessing()
    # pprint(re_cli.get_input())
    corr_flow = re_cli.start_flow(flow_input=flow_input)
    re_cli.progress(corr_flow['action_id'])
    pprint(re_cli.get_status(corr_flow['action_id']))
