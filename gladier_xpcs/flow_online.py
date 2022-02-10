from gladier import generate_flow_definition, utils
from gladier_xpcs.flow_base import XPCSBaseClient
from gladier_xpcs.tools.corr import eigen_corr
# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSOnlineFlow(XPCSBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    max_retries = 10
    containers = {
        utils.name_generation.get_funcx_function_name(eigen_corr): {
            'container_type': 'singularity',
            'location': '/eagle/APSDataAnalysis/XPCS/containers/eigen_v2.simg',
        }
    }

    gladier_tools = [
        'gladier_xpcs.tools.TransferFromClutchToTheta',
        'gladier_xpcs.tools.PrePublish',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.EigenCorr',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_xpcs.tools.Publish',
    ]
