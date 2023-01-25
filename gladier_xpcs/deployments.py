import datetime
import copy
from gladier_xpcs.collections import SharedCollection


apsdataanalysis = SharedCollection('f3305466-c63d-4a54-8bfc-624402c970bc',
                                           '/eagle/APSDataAnalysis/XPCS/', name='Gladier XPCS')
xpcs_data = SharedCollection('74defd5b-5f61-42fc-bcc4-834c9f376a4f',
                             '/eagle/XPCS-DATA-DYS/', name='XPCS Data 8-ID APS')
clutch = SharedCollection('fdc7e74a-fa78-11e8-9342-0e3d676669f4', '/', name='APS#Clutchsdmz')
theta_ep = SharedCollection('08925f04-569f-11e7-bef8-22000b9a448b', '/', name='alcf#dtn_theta')
apsdataprocessing = SharedCollection('98d26f35-e5d5-4edd-becf-a75520656c64', 
                                     '/eagle/APSDataProcessing/aps8idi/', name='APS8IDI')

class BaseDeployment:
    source_collection: SharedCollection = None
    staging_collection: SharedCollection = None
    pub_collection: SharedCollection = None
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
        'globus_endpoint_source': clutch.uuid,
        'globus_endpoint_proc': theta_ep.uuid,
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e449e8b8-e114-4659-99af-a7de06feb847',
        'funcx_endpoint_compute': '4c676cea-8382-4d5d-bc63-d6342bdb00ca',
    }

    flow_input = {
        'input': {
            'staging_dir': apsdataanalysis.path / 'data_online',
        }
    }


class NickPolarisGPU(BaseDeployment):

    source_collection = xpcs_data
    staging_collection = apsdataanalysis
    pub_collection = xpcs_data

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': apsdataanalysis.uuid,
    }

    funcx_endpoints = {
        # Theta login
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        # Containers
        'funcx_endpoint_compute': '4a6f2b52-d392-4a57-ad77-ae6e86daf503',
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'nick/xpcs_staging',
        }
    }


class NickPortalDeployment(BaseDeployment):

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

    source_collection = xpcs_data
    staging_collection = apsdataanalysis
    pub_collection = xpcs_data

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': apsdataanalysis.uuid,
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'hparraga/xpcs_staging',
        }
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'funcx_endpoint_compute': '3d9fde8a-1dfa-4ce7-93ab-5d524a59a4f6',
    }


class HannahPolaris(BaseDeployment):
    
    source_collection = xpcs_data
    staging_collection = apsdataanalysis
    pub_collection = xpcs_data

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': apsdataanalysis.uuid, 
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'funcx_endpoint_compute': '0676a1f2-b92f-41f7-8e4f-6cc93eb6f929',
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'hparraga/xpcs_staging',
        }
    }


class RyanPolaris(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': theta_ep.uuid,
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


class APS8IDIPolaris(BaseDeployment):

    source_collection = xpcs_data
    staging_collection = apsdataprocessing
    pub_collection = xpcs_data

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': apsdataprocessing.uuid,
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
        'funcx_endpoint_compute': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'xpcs_staging',
        }
    }


class RafPolaris(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': theta_ep.uuid,
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
    'nick-polaris-gpu': NickPolarisGPU(),
    'aps8idi-polaris': APS8IDIPolaris(),
}
