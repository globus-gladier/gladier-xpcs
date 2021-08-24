

class BaseDeployment:
    globus_endpoints = dict()
    funcx_endpoints = dict()
    flow_input = dict()

    def get_input(self):
        fi = self.flow_input.copy()
        fi['input'].update(self.funcx_endpoints)
        fi['input'].update(self.globus_endpoints)
        return fi


class NickDeployment(BaseDeployment):

    globus_endpoints = {
        'globus_endpoint_source': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
        'globus_endpoint_proc': '08925f04-569f-11e7-bef8-22000b9a448b',
        'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
    }

    funcx_endpoints = {
        'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
        'funcx_endpoint_compute': '2272d362-c13b-46c6-aa2d-bfb22255f1ba',
    }

    flow_input = {
        'input': {
            'proc_dir': '/projects/APSDataAnalysis/nick/gladier_testing/',
        }
    }


deployment_map = {
    'nick-testing': NickDeployment(),
}
