#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import argparse
import time
import sys
import os
import traceback
import pprint
from gladier.utils.flow_traversal import iter_flow
from gladier_xpcs.flows import XPCSEigen
from gladier_xpcs.flows import XPCSBoost

from globus_sdk import ConfidentialAppAuthClient, AccessTokenAuthorizer
from gladier.managers.login_manager import CallbackLoginManager

from typing import List, Mapping, Union

# Get client id/secret
CLIENT_ID = os.getenv("GLADIER_CLIENT_ID")
CLIENT_SECRET = os.getenv("GLADIER_CLIENT_SECRET")

# Set custom auth handler
def callback(scopes: List[str]) -> Mapping[str, Union[AccessTokenAuthorizer, AccessTokenAuthorizer]]:
    caac = ConfidentialAppAuthClient(CLIENT_ID, CLIENT_SECRET)
    response = caac.oauth2_client_credentials_tokens(requested_scopes=scopes)
    return {
        scope: AccessTokenAuthorizer(access_token=tokens["access_token"])
        for scope, tokens in response.by_scopes.scope_map.items()
    }

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", help="The automate flow instance(run) to check.",
                        default=None)
    parser.add_argument("--step", help="The inside the flow execution to check",
                        default=None)
    parser.add_argument("--interval", help="Interval between checking statuses", type=int,
                        default=90)
    parser.add_argument("--gpu", help="Whether the flow is the GPU flow rather than online flow.",
                        action='store_true', default=False)
    args = parser.parse_args()
    return args

class RunSucceeded(Exception):
    pass


class RunFailed(Exception):
    pass


def get_current_state_name(run_status):
    """Parse the state name from a flows run status response"""
    try:
        if run_status.get('state_name'):
            return run_status.get('state_name')
        elif run_status.get('details'):
            det = run_status.get('details')
            if det.get('details') and det['details'].get('state_name'):
                return run_status['details']['details']['state_name']
            elif det.get('details') and det['details'].get('output'):
                return list(det['details']['output'].keys())[0]
            elif det.get('action_statuses'):
                return run_status['details']['action_statuses'][0]['state_name']
            elif det.get('code') == 'FlowStarting':
                return None
    except Exception:
        print(traceback.format_exc(), file=sys.stderr)

    print(f'BUG ENCOUNTERED! An unexpected status was returned by the flows '
          f'service, a dump of the response is listed below. Please send this '
          f'to us so we can fix it. Thanks!', file=sys.stderr)
    pprint.pprint(run_status, stream=sys.stderr)
    sys.exit(2)


def state_has_completed(flow_state, flow_states, run_status):
    current_state = get_current_state_name(run_status)
    if current_state is None:
        return False
    return flow_states.index(current_state) > flow_states.index(flow_state)


def check_run_status(run_status):
    url = f'https://app.globus.org/runs/{run_status["run_id"]}'
    if run_status['status'] == 'FAILED':
        raise RunFailed(f'Run Failed: {url}')
    elif run_status['status'] == 'SUCCEEDED':
        raise RunSucceeded(f'Run Succeeded: {url}')


if __name__ == '__main__':
    args = arg_parse()

    callback_login_manager = None
    if CLIENT_ID and CLIENT_SECRET:
        callback_login_manager = CallbackLoginManager({}, callback=callback)

    if args.gpu:
        main_flow = XPCSBoost(login_manager=callback_login_manager)
    else:
        main_flow = XPCSEigen(login_manager=callback_login_manager)

    # Fetch state names in a loose ordering, depth first
    flow_states = [state_name for state_name, _ in iter_flow(main_flow.flow_definition)]

    if args.step not in flow_states:
        print(f'"{args.step}" is not valid, please choose from: {", ".join(flow_states)}')
        sys.exit(-1)

    try:
        while True:
            status = main_flow.get_status(args.run_id)
            check_run_status(status)
            if state_has_completed(args.step, flow_states, status):
                print(f'Step {args.step}: Completed')
                sys.exit(0)
            time.sleep(args.interval)
    except RunSucceeded as rs:
        print(str(rs))
        sys.exit(0)
    except (RunFailed) as e:
        print(f'{e.__class__.__name__}: {str(e)}', file=sys.stderr)
        sys.exit(1)