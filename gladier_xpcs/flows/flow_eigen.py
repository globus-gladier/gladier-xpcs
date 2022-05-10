"""
XPCS Eigen Flow

Summary: This flow executes standard XPCS flow using the EigenCorr container.
- Data is transfered from Clutch to Theta
- An "empty" publication is added to the index
- Nodes are "warmed"
- Eigen-Corr is applied
- Plots are made
- Gather + Publish the final data to the portal
"""
from gladier import generate_flow_definition, utils
from gladier_xpcs.flows.container_flow_base import ContainerBaseClient

from gladier_xpcs.tools.eigen_corr import eigen_corr
from gladier_xpcs.tools import TransferFromClutchToTheta
from gladier_xpcs.tools import PrePublish
from gladier_xpcs.tools import AcquireNodes
from gladier_xpcs.tools import EigenCorr
from gladier_xpcs.tools import MakeCorrPlots
from gladier_xpcs.tools import GatherXPCSMetadata
from gladier_xpcs.tools import Publish

# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSEigen(ContainerBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    containers = {
        utils.name_generation.get_funcx_function_name(eigen_corr): {
            'container_type': 'singularity',
            'location': '/eagle/APSDataAnalysis/XPCS/containers/eigen_v2.simg',
        }
    }

    gladier_tools = [
        TransferFromClutchToTheta,
        PrePublish,
        AcquireNodes,
        EigenCorr,
        MakeCorrPlots,
        GatherXPCSMetadata,
        Publish,
    ]
