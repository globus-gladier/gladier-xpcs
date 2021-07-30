from gladier import GladierBaseClient, generate_flow_definition

@generate_flow_definition(modifiers={
    # 'pre_publish_gather_metadata': {'WaitTime': 1200},
    # 'eigen_corr': {'WaitTime': 7200},
    # 'make_corr_plots':{'WaitTime': 7200},
    # 'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSReprocessingClient(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'

    gladier_tools = [
        # 'gladier_xpcs.tools.transfer_from_petrel_to_theta.TransferFromPetrelToTheta',
        'gladier_xpcs.tools.transfer_qmap.TransferQmap',
        # 'gladier_xpcs.tools.ApplyQmap',
        # 'gladier_xpcs.tools.EigenCorr',
        # 'gladier_xpcs.tools.MakeCorrPlots',
        # 'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        # 'gladier_xpcs.tools.Publish',
    ]
