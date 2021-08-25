

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
        }
    }


deployment_map = {
    'talc-prod': TalcDeployment(),
    'raf-cooley': RafCooleyDeployment(),
    'nick-testing': NickDeployment(),
}
