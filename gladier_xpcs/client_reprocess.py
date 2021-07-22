from gladier import GladierBaseClient, generate_flow_definition

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
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'

    gladier_tools = [
        'gladier_xpcs.tools.reprocessing.ManifestTransfer',
        'gladier_xpcs.tools.reprocessing.TransferQmap',
        'gladier_xpcs.tools.reprocessing.ManifestToList',
        'gladier_xpcs.tools.reprocessing.ManifestListToStateTasks',
        'gladier_xpcs.tools.ApplyQmap',
        'gladier_xpcs.tools.EigenCorr',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.CustomPilot',
    ]
