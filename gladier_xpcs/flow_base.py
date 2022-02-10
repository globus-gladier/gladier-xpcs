import logging
import time
import random
import globus_sdk
from gladier import GladierBaseClient, utils
# import gladier_xpcs.log  # Uncomment for debug logging

log = logging.getLogger(__name__)


class XPCSBaseClient(GladierBaseClient):
    # Allow all admins of the XPCS developers group to deploy/run flows
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'
    # This is a bit of a hack while we wait for Globus SDK v3, after that
    # it should automaically retry on these codes
    TRANSIENT_ERROR_STATUS_CODES = (429, 500, 502, 503, 504)
    max_retries = 10

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

    def retry_backoff(self, method, *args, **kwargs):
        retries = 0
        while retries < self.max_retries:
            try:
                return method(*args, **kwargs)
            except globus_sdk.GlobusTimeoutError:
                time.sleep(random.randint(1, 10))
            except globus_sdk.GlobusAPIError as gapie:
                if gapie.http_status not in self.TRANSIENT_ERROR_STATUS_CODES:
                    raise
                time.sleep(random.randint(1, 10))
        raise

    def run_flow(self, *args, **kwargs):
        return self.retry_backoff(super().run_flow, *args, **kwargs)

    def get_status(self, *args, **kwargs):
        return self.retry_backoff(super().get_status, *args, **kwargs)