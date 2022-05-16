"""
XPCS Boost Flow

Summary: This flow executes new xpcs_boost CPU/GPU flow.
- Data is transfered from Clutch to Theta
- An "empty" publication is added to the index
- Nodes are "warmed"
- Boost Multitau and/or Twotime is applied using CPU/GPU
- Plots are made
- Gather + Publish the final data to the portal
"""

from gladier import GladierBaseClient, generate_flow_definition

##Import individual functions
from gladier_xpcs.tools import TransferFromClutchToTheta
from gladier_xpcs.tools import PrePublish
from gladier_xpcs.tools import AcquireNodes
from gladier_xpcs.tools import BoostCorr
from gladier_xpcs.tools import MakeCorrPlots
from gladier_xpcs.tools import GatherXPCSMetadata
from gladier_xpcs.tools import Publish

# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
   'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSBoost(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    gladier_tools = [
        TransferFromClutchToTheta,
        PrePublish,
        AcquireNodes,
        BoostCorr,
        MakeCorrPlots,
        GatherXPCSMetadata,
        Publish,
    ]
