from gladier import generate_flow_definition
# import gladier_xpcs.log  # Uncomment for debug logging
from gladier_xpcs.flow_base import XPCSBaseClient


@generate_flow_definition(modifiers={
   'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSGPUFlow(XPCSBaseClient):
    # See flow_base.py for assigning containers.
    containers = {}

    gladier_tools = [
        'gladier_xpcs.tools.TransferFromClutchToTheta',
        'gladier_xpcs.tools.PrePublish',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.EigenCorrGPU',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_xpcs.tools.Publish',
    ]
