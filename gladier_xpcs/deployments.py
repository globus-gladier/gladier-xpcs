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
voyagerXPCS8 = SharedCollection('d2dce3ad-0550-4c90-a2a9-6c6812b7d845', 
                                     '/gdata/dm/XPCS8/', name='APS:DM-TEST:XPCS8 Guest')
voyager8IDI = SharedCollection('dc86d51b-81d1-4827-81be-2b5e64ba7dc1', 
                                     '/gdata/dm/8IDI/', name='APS:DM:8IDI Guest')
nersc_permutter = SharedCollection('6bdc7956-fc0f-4ad2-989c-7aa5ee643a79', 
                                     '/', name='NERSC#Perlmutter')

class BaseDeployment:
    source_collection: SharedCollection = None
    staging_collection: SharedCollection = None
    pub_collection: SharedCollection = None
    globus_endpoints = dict()
    compute_endpoints = dict()
    flow_input = dict()
    # Is this a "service account" that requires confidential client credentials?
    # This means setting GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET
    service_account = False

    def get_input(self):
        fi = self.flow_input.copy()
        fi['input'].update(self.compute_endpoints)
        fi['input'].update(self.globus_endpoints)
        return fi

class Talc(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': clutch.uuid,
        'globus_endpoint_proc': theta_ep.uuid,
    }

    compute_endpoints = {
        'login_node_endpoint': 'e449e8b8-e114-4659-99af-a7de06feb847',
        'compute_endpoint': '4c676cea-8382-4d5d-bc63-d6342bdb00ca',
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

    compute_endpoints = {
        # Theta login
        # 'login_node_endpoint': '553e7b64-0480-473c-beef-be762ba979a9',
        # # Containers
        # 'compute_endpoint': '4a6f2b52-d392-4a57-ad77-ae6e86daf503',
        'login_node_endpoint': '2fe84982-1c50-45bf-8d68-e4b659c128cb',
        'compute_endpoint': '2fe84982-1c50-45bf-8d68-e4b659c128cb',
    }

    function_ids = {
        "acquire_nodes_function_id": "1f937cb4-7276-4867-bb9f-6c42031be090",
        "xpcs_boost_corr_function_id": "d95600d7-2f5c-4453-8d25-655b8429e839",
        "make_corr_plots_function_id": "7245d81b-4530-4ee8-b246-4ad043a71da2",
        "gather_xpcs_metadata_function_id": "a6570e9f-69a3-470a-bc3d-39f87d794f82",
        "publish_gather_metadata_function_id": "5756975d-0b08-4f5f-8314-1417c6e3db54"
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'nick/xpcs_staging',
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

    compute_endpoints = {
        'login_node_endpoint': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'compute_endpoint': '3d9fde8a-1dfa-4ce7-93ab-5d524a59a4f6',
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

    compute_endpoints = {
        'login_node_endpoint': 'e3e1aef6-0a6f-4ef1-b9c6-a14b0efb1dfa',
        'compute_endpoint': '0676a1f2-b92f-41f7-8e4f-6cc93eb6f929',
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

    compute_endpoints = {
        'login_node_endpoint': '6c4323f4-a062-4551-a883-146a352a43f5',
        'compute_endpoint': 'dc2a0cdb-2aee-44f7-a422-c4e28d9f7617',
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
    service_account = True

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': apsdataprocessing.uuid,
    }

    compute_endpoints = {
        'login_node_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
        'compute_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'xpcs_staging',
        }
    }

    function_ids = {
        'acquire_nodes_function_id': '528fe875-ab52-4da7-a690-57aacb8392c9',
        'xpcs_boost_corr_function_id': '4db55ea5-0691-49a0-9e13-29823992f0de',
        'make_corr_plots_function_id': '09cc13bc-97db-4c3a-a3a0-da81c588042a',
        'gather_xpcs_metadata_function_id': '76789fc3-e464-4d2b-a0be-62a6a6354c69',
        'publish_gather_metadata_function_id': '35f120b3-ccf3-4bc2-89fb-002e173a8f2f',
    }

class RyanNERSC(BaseDeployment):

    source_collection = xpcs_data
    staging_collection = nersc_permutter
    pub_collection = xpcs_data
    service_account = False

    globus_endpoints = {
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_source': xpcs_data.uuid,
        'globus_endpoint_proc': nersc_permutter.uuid,
    }

    compute_endpoints = {
        'login_node_endpoint': '398d7225-0015-4270-b40c-5e9f9044127e',
        'compute_endpoint': '398d7225-0015-4270-b40c-5e9f9044127e',
    }

    flow_input = {
        'input': {
            'staging_dir': '/global/homes/r/rchard/xpcs_staging',
        }
    }

    function_ids = {
        'acquire_nodes_function_id': 'dc9496aa-51b4-4e7b-98d9-62208204acff',
        'xpcs_boost_corr_function_id': '28ca0dc6-624d-4403-8d99-54c0e130624f',
        'make_corr_plots_function_id': '4d14f282-9e2c-44cb-9b90-91804c88cdfc',
        'gather_xpcs_metadata_function_id': '9bbd0c81-6f4c-4666-aab8-1a0ed5cc80b5',
        'publish_gather_metadata_function_id': 'c57f91a8-56e4-432c-871c-efb4d20fc87f',
    }


class VoyagerXPCS8Polaris(BaseDeployment):

    source_collection = voyagerXPCS8
    staging_collection = apsdataprocessing
    pub_collection = xpcs_data
    service_account = True

    globus_endpoints = {
        # Voyager -- APS:DM:XPCS8
        'globus_endpoint_source': voyagerXPCS8.uuid,
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_proc': apsdataprocessing.uuid,
    }

    compute_endpoints = {
        'login_node_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
        'compute_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
    }

    flow_input = {
        'input': {
            'staging_dir': staging_collection.path / 'xpcs_staging',
        }
    }


class Voyager8IDIPolaris(BaseDeployment):

    source_collection = voyager8IDI
    staging_collection = apsdataprocessing
    pub_collection = xpcs_data
    service_account = True

    globus_endpoints = {
        # Voyager -- APS:DM:8IDI
        'globus_endpoint_source': voyager8IDI.uuid,
        # Eagle -- XPCS Data 8-ID APS
        'globus_endpoint_proc': apsdataprocessing.uuid,
    }

    compute_endpoints = {
        'login_node_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
        'compute_endpoint': 'f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec',
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

    compute_endpoints = {
        'login_node_endpoint': 'e449e8b8-e114-4659-99af-a7de06feb847',
        'compute_endpoint': 'a93b6438-6ff7-422e-a1a2-9a4c6d9c1ea5',
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
    'voyager-8idi-polaris': Voyager8IDIPolaris(),
    'voyager-xpcs8-polaris': VoyagerXPCS8Polaris(),
    # This is a hack for the SC demo in case we can't get nodes. You can remove this after Nov 16th, 2023.
    'aps8idi-polaris-backup': NickPolarisGPU(),
    'nersc': RyanNERSC(),
}
