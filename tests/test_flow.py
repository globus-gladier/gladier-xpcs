import os
import sys
import time
import argparse

from globus_automate_client import create_flows_client, FlowsClient
from globus_sdk import ConfidentialAppAuthClient, ClientCredentialsAuthorizer, AccessTokenAuthorizer

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')

MANAGE_FLOWS_SCOPE = (
    "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/manage_flows"
)
VIEW_FLOWS_SCOPE = (
    "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/view_flows"
)
RUN_FLOWS_SCOPE = (
    "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run"
)
RUN_STATUS_SCOPE = (
    "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_status"
)
RUN_MANAGE_SCOPE = (
    "https://auth.globus.org/scopes/eec9b274-0c81-4334-bdc2-54e90e689b9a/run_manage"
)

SCOPES = [MANAGE_FLOWS_SCOPE, VIEW_FLOWS_SCOPE, RUN_FLOWS_SCOPE, RUN_STATUS_SCOPE, RUN_MANAGE_SCOPE]
FLOW_ID = 'd8f84b43-ecb5-41d5-927e-aaef4e19107e'
FLOW_SCOPE = 'https://auth.globus.org/scopes/d8f84b43-ecb5-41d5-927e-aaef4e19107e/flow_d8f84b43_ecb5_41d5_927e_aaef4e19107e_user'
TOKENS = None


def create_flow(fc, flow_id, flow_scope):
    """Create a flow
    """
    
    flow_definition = {
        "Comment": "A test flow",
        "StartAt": "Run",
        "States": {
            "Run": {
                "Comment": "Run a funcX function",
                "Type": "Action",
                "ActionUrl": "https://automate.funcx.org",
                "ActionScope": "https://auth.globus.org/scopes/b3db7e59-a6f1-4947-95c2-59d6b7a70f8c/action_all",
                "Parameters": {
                    "tasks": [{
                        "endpoint.$": "$.input.fx_ep",
                        "function.$": "$.input.fx_id",
                        "payload": {
                            "data.$": "$.input.data"
                        }
                    }]
                },
                "ResultPath": "$.Result",
                "WaitTime": 600,
                "End": True
            }
        }
    }

    flow = fc.deploy_flow(flow_definition, title="Github test flow", input_schema={})
    flow_id = flow['id']
    flow_scope = flow['globus_auth_scope']
    print(f'Newly created flow with id:\n{flow_id}\nand scope:\n{flow_scope}')

    return flow_id, flow_scope


def run_and_monitor(fc, flow_id, flow_scope):
    """Run the flow and check its output
    """
    src_ep = 'ddb59aef-6d04-11e5-ba46-22000b92c6ec' # EP1
    dest_ep = 'ddb59af0-6d04-11e5-ba46-22000b92c6ec' # EP2
    filename = 'test.txt'

    flow_input = {
        "input": {
            "fx_id": 'bec5066b-b324-4a2b-8ed6-b8ce24910487',
            "fx_ep": '4b116d3c-1703-4f8f-9f6f-39921e5864df',
            "data": 'Hello'
        }
    }
    flow_action = fc.run_flow(flow_id, flow_scope, flow_input)

    flow_action_id = flow_action['action_id']
    flow_status = flow_action['status']
    print(f'Flow action started with id: {flow_action_id}')

    while flow_status == 'ACTIVE':
        time.sleep(10)
        flow_action = fc.flow_action_status(flow_id, flow_scope, flow_action_id)
        flow_status = flow_action['status']
        print(f'Flow status: {flow_status}')

    return flow_status


def authorizer_callback(*args, **kwargs):
    auth = AccessTokenAuthorizer(
        TOKENS.by_resource_server[FLOW_ID]['access_token']
    )
    return auth


def start(client_id, client_secret, flow_id=None, flow_scope=None):
    """Use a confidential client to run and monitor a flow.
    """
    global CLIENT_ID
    global CLIENT_SECRET
    global TOKENS
    global FLOW_ID

    if not CLIENT_ID:
        CLIENT_ID = client_id
    if not CLIENT_SECRET:
        CLIENT_SECRET = client_secret

    confidential_client = ConfidentialAppAuthClient(
        client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    if flow_scope:
        SCOPES.append(FLOW_SCOPE)
    
    TOKENS = confidential_client.oauth2_client_credentials_tokens(requested_scopes=SCOPES)

    cca = ClientCredentialsAuthorizer(
        confidential_client,
        MANAGE_FLOWS_SCOPE,
        TOKENS.by_resource_server['flows.globus.org']['access_token'],
        TOKENS.by_resource_server['flows.globus.org']['expires_at_seconds']
    )
    
    fc = FlowsClient.new_client(
        client_id=CLIENT_ID,
        authorizer_callback=authorizer_callback,
        authorizer=cca)

    #fc.delete_flow(FLOW_ID)
    #flow_id, flow_scope = create_flow(fc, flow_id, flow_scope)
    res = run_and_monitor(fc, FLOW_ID, FLOW_SCOPE)

    if res == "SUCCEEDED":
        return
    else:
        raise Exception("Flow did not succeed")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--id", required=True,
                        help="CLIENT_ID for Globus")
    parser.add_argument("-s", "--secret", required=True,
                        help="CLIENT_SECRET for Globus")
    args = parser.parse_args()
    
    client_id = args.id
    client_secret = args.secret

    start(client_id, client_secret, FLOW_ID, FLOW_SCOPE)

