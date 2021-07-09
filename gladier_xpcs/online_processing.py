from gladier import GladierBaseClient, generate_flow_definition


@generate_flow_definition(modifiers={
#    'pre_publish_gather_metadata': {'WaitTime': 600},
#    'eigen_corr': {'WaitTime': 3600},
#    'make_corr_plots':{'WaitTime': 3600},
#    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result'}
})
class XPCSClient(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'

    gladier_tools = [
#        'gladier_xpcs.tools.transfer_from_clutch_to_theta.TransferFromClutchToTheta',
#        'gladier_xpcs.tools.pre_publish.PrePublish',
        'gladier_xpcs.tools.EigenCorr',
#        # 'gladier_xpcs.tools.transfer_from_clutch_to_theta.TransferToClutch',
#        'gladier_xpcs.tools.plot.MakeCorrPlots',
#        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
#        'gladier_tools.publish.Publish',
    ]
