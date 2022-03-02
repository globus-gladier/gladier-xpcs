import logging
import time
import random
import globus_sdk
from gladier import GladierBaseClient, utils
# import gladier_xpcs.log  # Uncomment for debug logging

log = logging.getLogger(__name__)


class ContainerBaseClient(GladierBaseClient):
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