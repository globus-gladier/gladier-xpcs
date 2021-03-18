from gladier.client import GladierClient
from payload import * 

class XPCS_Client(GladierClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.xpcs.EigenCorr',
        'gladier_tools.xpcs.ApplyQmap',
    ]
    flow_definition = '.flow_def.corr_basic_flow_definition'


if __name__ == '__main__':

    corr_cli = XPCS_Client()
    corr_flow = corr_cli.run_flow(flow_input=data)
    corr_cli.check_flow(corr_flow['action_id'])
