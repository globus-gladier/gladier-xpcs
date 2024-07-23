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
import os
from gladier import GladierBaseClient, generate_flow_definition, utils
from gladier_xpcs.flows.container_flow_base import ContainerBaseClient
from gladier_xpcs.deployments import deployment_map

# import gladier_xpcs.log  # Uncomment for debug logging


@generate_flow_definition(modifiers={
   'publishv2_gather_metadata': {
       'payload': '$.GatherXpcsMetadata.details.results[0].output',
       'WaitTime': 600,
    }
})
class XPCSBoost(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    
    gladier_tools = [
        'gladier_xpcs.tools.SourceTransfer',
        'gladier_xpcs.tools.AcquireNodes',
        'gladier_xpcs.tools.BoostCorr',
        'gladier_xpcs.tools.ResultTransfer',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_tools.publish.Publishv2',
    ]
