from gladier import GladierBaseClient, generate_flow_definition
# This is a HACK to enable glaider logging
import gladier.tests
from pprint import pprint


@generate_flow_definition(modifiers={
    'list_to_fx_tasks': {
        'payload': {
            'states': [
                {
                    'name': 'ApplyQmaps',
                    'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                    'funcx_id.$': '$.input.apply_qmap_funcx_id',
                },
                {
                    'name': 'EigenCorr',
                    'funcx_endpoint.$': '$.input.funcx_endpoint_compute',
                    'funcx_id.$': '$.input.eigen_corr_funcx_id',
                },
                {
                    'name': 'MakeCorrPlots',
                    'funcx_endpoint.$': '$.input.funcx_endpoint_compute',
                    'funcx_id.$': '$.input.make_corr_plots_funcx_id',
                },
                {
                    'name': 'CustomPilot',
                    'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                    'funcx_id.$': '$.input.custom_pilot_funcx_id',
                }
            ],
            # 'payloads.$': '$.input.manifest_list_test',
            'payloads.$': '$.ManifestToList.details.result'
        }
    },
    'apply_qmap': {'InputPath': '$.ListToFxTasks.details.result.ApplyQmaps'},
    # 'apply_qmap': {'InputPath': '$.input.apply_qmaps_test'}
    'eigen_corr': {'InputPath': '$.ListToFxTasks.details.result.EigenCorr'},
    'make_corr_plots': {'InputPath': '$.ListToFxTasks.details.result.MakeCorrPlots'},
    'custom_pilot': {'InputPath': '$.ListToFxTasks.details.result.CustomPilot'},
})
class XPCSReprocessing(GladierBaseClient):
    gladier_tools = [
        'gladier_xpcs.tools.reprocessing.manifest_transfer.ManifestTransfer',
        'gladier_xpcs.tools.reprocessing.transfer_qmap.TransferQmap',
        'gladier_xpcs.tools.reprocessing.manifest_to_list.ManifestToList',
        'gladier_xpcs.tools.reprocessing.manifest_list_to_state_tasks.ManifestListToStateTasks',
        'gladier_xpcs.tools.reprocessing.apply_qmap.ApplyQmap',
        'gladier_xpcs.tools.reprocessing.corr.EigenCorr',
        'gladier_xpcs.tools.reprocessing.plot.MakeCorrPlots',
        'gladier_xpcs.tools.reprocessing.custom_pilot.CustomPilot',
    ]


if __name__ == '__main__':
    flow_input = {
        'input': {
            # Manifest input files, and destination for where to process (Anyone can use the manifest below)
            'manifest_id': '55dc53cf-593a-40d0-aab6-fcea1c1d05a4',
            'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/nick/gladier_testing/',

            # Corr inputs
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'flags': '',

            # Qmap inputs
            'qmap_source_endpoint': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
            'qmap_source_path': '/XPCSDATA/Automate/qmap/sanat201903_qmap_S270_D54_lin.h5',
            'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
            'qmap_file': '/projects/APSDataAnalysis/nick/gladier_testing/sanat201903_qmap_S270_D54_lin.h5',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',

            # Pilot inputs (Renames the dataset)
            'reprocessing_suffix': '_nick_reprocessing_test',

            # Funcx excecution (Note: You need to update this to your endpoints...)
            # 'funcx_endpoint_non_compute': '0a162a09-8bd9-4dd9-b046-0bfd635d38a7',
            # 'funcx_endpoint_compute': '37e6099f-e9e7-4817-ac68-4afcd78d8221',

            # Useful for turning off "manifest_to_list" and using a custom payload
            'parameter_file': '/projects/APSDataAnalysis/nick/gladier_testing/A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/parameters.json',

            # Needed for corr and plots when running standalone
            'proc_dir': '/lus/theta-fs0/projects/APSDataAnalysis/nick/gladier_testing',
            'hdf_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap.hdf',
            'imm_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm',
        }
    }

    re_cli = XPCSReprocessing()
    pprint(re_cli.flow_definition)
    # re_cli.logout()
    pprint(re_cli.get_input())

    corr_flow = re_cli.run_flow(flow_input=flow_input)
    action_id = corr_flow['action_id']

    re_cli.progress(action_id)
    pprint(re_cli.get_status(action_id)) 