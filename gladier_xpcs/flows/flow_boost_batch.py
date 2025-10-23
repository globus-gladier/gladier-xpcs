from gladier import GladierBaseClient, generate_flow_definition
import gladier.tests

@generate_flow_definition(modifiers={
    "xpcs_boost_corr": {
        "tasks": "$.input.xpcs_boost_corr_tasks",
    }
})
class XPCSBoostBatch(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    action_url = "https://compute.actions.globus.org/v3"
    
    gladier_tools = [
        'gladier_xpcs.tools.source_transfer.SourceTransfer',
        'gladier_xpcs.tools.batch_corr_setup.BatchCorrSetup',
        'gladier_xpcs.tools.xpcs_boost_corr.BoostCorr',
        # These items are not supported yet. Batching produces a ton of outputs spread across directories
        # All of the tools below need to be rewritten to handle the batch of directories created
        # 'gladier_xpcs.tools.result_transfer.ResultTransfer',
        # 'gladier_xpcs.tools.boost_corr_runtime.BoostCorrRuntime',
        # 'gladier_xpcs.tools.plot.MakeCorrPlots',
        # 'gladier_tools.publish.Publishv2',
    ]
