from gladier import GladierBaseClient, generate_flow_definition

@generate_flow_definition(modifiers={
#    'publishv2_gather_metadata': {
#        'payload': '$.GatherXpcsMetadata.details.results[0].output',
#        'WaitTime': 600,
#     }
})
class XPCSBoost(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    
    gladier_tools = [
        'gladier_xpcs.tools.source_transfer.SourceTransfer',
        'gladier_xpcs.tools.xpcs_boost_corr.BoostCorr',
        'gladier_xpcs.tools.result_transfer.ResultTransfer',
        'gladier_xpcs.tools.plot.MakeCorrPlots',
        'gladier_tools.publish.Publishv2',
    ]
