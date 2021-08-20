"""
XPCS Reprocessing

Summary: Data which has already been published on petrel is transferred back into theta,
along with a custom QMap file. The QMap file is applied to the original HDF, and the
new HDF from that changes the behavior of Corr. Corr is then run normally and plots are
created. After that, PublishPreparation will rename the dataset so it doesn't overwrite
the originally published dataset in Globus Search. Some additional reprocessing specific
metadata is added along with standard XPCS metadata. Publish then ingests data into Globus
Search and places data on petrel (probably eagle now), which makes it viewable through
the portal at https://petreldata.alcf.anl.gov
"""
from gladier import GladierBaseClient, generate_flow_definition


@generate_flow_definition(modifiers={
    'gather_xpcs_metadata': {'payload': '$.PublishPreparation.details.result[0]'},
    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSReprocessingClient(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'

    gladier_tools = [
        # Transfer XPCS files into our processing environment
        'gladier_xpcs.reprocessing_tools.transfer_to_proc.TransferToProc',
        'gladier_xpcs.reprocessing_tools.transfer_qmap.TransferQmap',
        # Apply custom hdf settings to pass info into corr
        'gladier_xpcs.reprocessing_tools.apply_qmap.ApplyQmap',
        'gladier_xpcs.tools.WarmNodes',
        'gladier_xpcs.tools.EigenCorr',
        'gladier_xpcs.tools.MakeCorrPlots',

        # Change the name of the dataset according to the reprocessing suffix
        # and fetch some extra metadata
        'gladier_xpcs.reprocessing_tools.publish_preparation.PublishPreparation',
        # Gather XPCS specific metadata from the HDF file
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        # Publish using pilot to the Globus Search index
        'gladier_xpcs.tools.Publish',
    ]
