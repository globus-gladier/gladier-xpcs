import datetime
import copy


class BaseDeployment:
    globus_endpoints = dict()
    funcx_endpoints = dict()
    flow_input = dict()

    def get_input(self):
        fi = self.flow_input.copy()
        fi['input'].update(self.funcx_endpoints)
        fi['input'].update(self.globus_endpoints)
        return fi

class TalcDeployment(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e449e8b8-e114-4659-99af-a7de06feb847',
        'funcx_endpoint_compute': '4c676cea-8382-4d5d-bc63-d6342bdb00ca',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/XPCS/data_online',
        }
    }

class RafCooleyDeployment(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': '1d5c6081-d716-4b94-b00b-661409876688',
        'funcx_endpoint_compute':     'f8f611a1-0c45-4bf6-87c5-e85b9fb4d7c0',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/XPCS_test/cooley_raf',
        }
    }



class NickDeployment(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        'funcx_endpoint_compute': '2272d362-c13b-46c6-aa2d-bfb22255f1ba',
    }

    flow_input = {
        'input': {
            'staging_dir': '/projects/APSDataAnalysis/nick/gladier_testing/',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
        }
    }


class NickTalc(NickDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }


class NickPortalDeployment(NickDeployment):

    def get_input(self):
        """Separate portal runs by current datetime/second. This prevents run collisions
        of mulitple people running flows at the same time"""
        now = datetime.datetime.now()
        now = now - datetime.timedelta(microseconds=now.microsecond)
        now = now.isoformat().replace(':', '')

        finput = copy.deepcopy(super().get_input())
        finput['input']['staging_dir'] = finput['input']['staging_dir'].format(now=now)
        return finput

    flow_input = {
        'input': {
            'staging_dir': '/projects/APSDataAnalysis/XPCS/portal/{now}/',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            # We don't have a way to store authorization data within a database yet.
            # FuncX ids on the portal need to be specified manually
            'apply_qmap_funcx_id': '4d15c42d-a982-46ed-a548-497ac5977b70',
            'eigen_corr_funcx_id': 'df859253-1113-4cbc-820c-8cf4afbf5764',
            'gather_xpcs_metadata_funcx_id': '348e7fe6-7d64-4ccf-84b0-502294a087e9',
            'make_corr_plots_funcx_id': 'dba85394-eae5-4651-827e-1cf03f536a75',
            'publish_gather_metadata_funcx_id': '9a36d48b-b072-4e7d-a2dc-8f4a31ef9b45',
            'publish_preparation_funcx_id': '4b39dbd5-1954-4923-89e3-9abbb39c0375',
            'warm_nodes_funcx_id': 'f369eb60-9a4c-49cb-a078-abb1a81a7c66'
        }
    }



deployment_map = {
    'talc-prod': TalcDeployment(),
    'raf-cooley': RafCooleyDeployment(),
    'nick-testing': NickDeployment(),
    'nick-talc': NickTalc(),
}
