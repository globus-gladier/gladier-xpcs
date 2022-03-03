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

class Talc(BaseDeployment):

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


class NickTheta(BaseDeployment):
    """Nicks deployment on theta"""

    globus_endpoints = {
        # Clutch DMZ
        'globus_endpoint_source': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        # Theta Login
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        # Theta Compute
        'funcx_endpoint_compute': '2272d362-c13b-46c6-aa2d-bfb22255f1ba',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/nick/xpcs',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
        }
    }


class NickPolaris(NickTheta):

    funcx_endpoints = {
        # Theta login
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        # Polaris Compute
        # 'funcx_endpoint_compute': '9a291fa2-3542-42b7-91d6-f80b44629cfa',
        # Containers
        'funcx_endpoint_compute': 'd2659fc0-0454-4af1-97ec-012771c869f9',
    }

class NickPolarisGPU(NickPolaris):

    funcx_endpoints = {
        # Theta login
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        # Polaris Compute
        # 'funcx_endpoint_compute': '9a291fa2-3542-42b7-91d6-f80b44629cfa',
        # Containers
        'funcx_endpoint_compute': '58f83203-fc24-4d47-a7f7-09342f320312',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/nick/xpcs_gpu',
        }
    }


class NickCooley(NickTheta):

    funcx_endpoints = {
        # Theta login
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        # Cooley Compute
        'funcx_endpoint_compute': '9a291fa2-3542-42b7-91d6-f80b44629cfa',
    }


class NickPortalDeployment(NickTheta):

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

class HannahTheta(BaseDeployment):
    globus_endpoints = {
        'globus_endpoint_source': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'funcx_endpoint_compute': '3d9fde8a-1dfa-4ce7-93ab-5d524a59a4f6',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/projects/APSDataAnalysis/XPCS/hparraga/gladier_testing/',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
        }
    }

class HannahPolaris(HannahTheta):
    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'funcx_endpoint_compute': '0bbf9fe3-0cae-43bd-9307-625c9b07e3b6',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/projects/APSDataAnalysis/XPCS/hparraga/gladier_testing/',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'corr_gpu_loc' : '/eagle/projects/APSDataAnalysis/XPCS/mchu/xpcs_boost/gpu_corr.py',
        }
    }

class RyanPolaris(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': '6c4323f4-a062-4551-a883-146a352a43f5',
        'funcx_endpoint_compute': 'dc2a0cdb-2aee-44f7-a422-c4e28d9f7617',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/rchard/xpcs/',
            'corr_loc': '/eagle/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
        }
    }


class RafPolaris(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e449e8b8-e114-4659-99af-a7de06feb847',
        'funcx_endpoint_compute': 'a93b6438-6ff7-422e-a1a2-9a4c6d9c1ea5',
    }

    flow_input = {
        'input': {
            'staging_dir': '/eagle/APSDataAnalysis/XPCS/raf/xpcs/',
        }
    }

deployment_map = {
    'talc-prod': Talc(),
    'raf-polaris': RafPolaris(),
    'hannah-theta': HannahTheta(),
    'hannah-polaris': HannahPolaris(),
    'ryan-polaris': RyanPolaris(),
    'nick-theta': NickTheta(),
    'nick-cooley': NickCooley(),
    'nick-polaris': NickPolaris(),
    'nick-polaris-gpu': NickPolarisGPU(),
}
