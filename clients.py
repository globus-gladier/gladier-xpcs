from gladier import GladierBaseClient
from flow_defs import corr_basic_flow_definition, qmap_flow_definition, flow_definition

class XPCS_Corr_Client(GladierBaseClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.xpcs.EigenCorr',
    ]
    flow_definition = corr_basic_flow_definition ##TODO change that to the original flow ryan was using
##

class XPCS_Qmap_Client(GladierBaseClient):
    client_id = 'e6c75d97-532a-4c88-b031-8584a319fa3e'
    gladier_tools = [
        'gladier_tools.xpcs.ApplyQmap',
    ]
    flows[0] = qmap_flow_definition ##TODO change that to the original flow ryan was using
##
