import sys
import argparse
from gladier import GladierBaseClient
from globus_sdk.exc.convert import GlobusConnectionError


class FlowExecutionError(Exception):
    pass

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--flowID', required=True)
    return parser.parse_args()

def getStatus(client, flowID):
    ''' for intermittent connection errors, recursively retry status request '''
    try:
        return client.get_status(flowID)
    except GlobusConnectionError as e:
        print(f"Resubmitting flow, Globus Connection Error: {e}")
        return getStatus(client, flowID)
            
if __name__ == '__main__':
    args = arg_parse()
    client = GladierBaseClient()
    status = getStatus(client, args.flowID)
    print(f"Status: {status.get('status')}")
    if status.get('status') == "FAILED":
        raise FlowExecutionError(f"ERROR: Gladier flow failed. {status}")
