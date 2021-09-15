import logging
from gladier import GladierBaseClient, generate_flow_definition, utils
from gladier_xpcs.tools.corr import eigen_corr
# import gladier_xpcs.log  # Uncomment for debug logging

log = logging.getLogger(__name__)


@generate_flow_definition(modifiers={
    'publish_gather_metadata': {'payload': '$.GatherXpcsMetadata.details.result[0]'}
})
class XPCSOnlineFlow(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    containers = {
        utils.name_generation.get_funcx_function_name(eigen_corr): {
            'container_type': 'singularity',
            'location': '/eagle/APSDataAnalysis/XPCS/containers/eigen_v2.simg',
        }
    }

    gladier_tools = [
        'gladier_xpcs.tools.TransferFromClutchToTheta',
        'gladier_xpcs.tools.PrePublish',
        'gladier_xpcs.tools.EigenCorr',
        # 'gladier_xpcs.tools.transfer_from_clutch_to_theta.TransferToClutch',
        'gladier_xpcs.tools.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_xpcs.tools.Publish',
    ]

    def register_funcx_function(self, function):
        """Register containers for any functions listed in self.containers"""
        fxid_name = utils.name_generation.get_funcx_function_name(function)
        if fxid_name in self.containers.keys():
            container = self.containers[fxid_name]
            log.info(f'Registering {fxid_name} with {container["location"]}')
            fxid_name = utils.name_generation.get_funcx_function_name(function)
            fxck_name = utils.name_generation.get_funcx_function_checksum_name(function)
            cfg = self.get_cfg(private=True)
            cid = self.funcx_client.register_container(**container)
            fxid = self.funcx_client.register_function(function, function.__doc__, container_uuid=cid)
            cfg[self.section][fxid_name] = fxid
            cfg[self.section][fxck_name] = self.get_funcx_function_checksum(function)
            cfg.save()
        else:
            super().register_funcx_function(function)
