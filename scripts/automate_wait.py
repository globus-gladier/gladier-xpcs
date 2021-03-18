#!/home/beams/8IDIUSER/.conda/envs/automate/bin/python
# -*- coding: utf-8 -*-

"""
Automate XPCS: Initiate processing of an XPCS file.
"""

import argparse
import warnings
import logging
import time
import json
import sys
import os

from os.path import expanduser
from glob import glob

from globus_automate_client import create_flows_client

logging.basicConfig(filename='automate.log', level=logging.WARNING, format='%(asctime)s: %(message)s')

# flow details
FLOW_ID = "37488591-c036-4756-8a88-194578d425ba"
FLOW_SCOPE = "https://auth.globus.org/scopes/37488591-c036-4756-8a88-194578d425ba/flow_37488591_c036_4756_8a88_194578d425ba"

def main(arg):
    """
    Wait for the task id to complete
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id", help="The automate flow to process the data.",
                        default=None)
    parser.add_argument("--flow_id", help="The automate flow to process the data.",
                        default=FLOW_ID)
    parser.add_argument("--flow_scope", help="The automate flow scope to use to process the data.",
                        default=FLOW_SCOPE)
    parser.add_argument("--step", help="The step to wait on. Options: Transfer1, ExecCorr, Transfer2, ExecPlots, ExecPilot",
                        default=None)
    parser.add_argument("--walltime", help="How long to wait before exiting.",
                        default=None)
    args = parser.parse_args()

    CLIENT_ID = "e6c75d97-532a-4c88-b031-8584a319fa3e"
    flows_client = create_flows_client(CLIENT_ID)

    flow_response = automate_wait(flows_client, args.flow_id, args.flow_scope, args.task_id, args.step, args.walltime)

    if flow_response == "SUCCEEDED":
        sys.exit(0)
    else:
        sys.exit(1)


def automate_wait(flows_client, flow_id, flow_scope, flow_action_id, wait_step, walltime):
    """
    Initiate the flow.
    """
    start_time = time.time()
    flow_status = 'ACTIVE'
    # Define the stages so we can check if the --step field has finished
    flow_stages = ['Transfer1', 'ExecCorr', 'Transfer2', 'ExecPlots', 'ExecPilot', 'End']
    wait_stage = None
    if wait_step:
        wait_stage = flow_stages.index(wait_step)

    while flow_status == 'ACTIVE':
        # Break if the walltime has passed
        if walltime:
            cur_time = time.time()
            if int(cur_time - start_time) >= int(walltime):
                return "FAILED"

        flow_action = flows_client.flow_action_status(flow_id, flow_scope, flow_action_id)
        flow_status = flow_action['status']
        flow_state = 'DONE'

        try:
            flow_state = flow_action.data['details']['details']['state_name']
        except Exception as e:
            pass

        if wait_step:
            flow_log = flows_client.flow_action_log(flow_id, flow_scope, flow_action_id)
            for l in flow_log['entries']:
                if 'details' in l and 'state_name' in l['details']:
                    # Check if the stage is later than the one we are waiting on
                    if l['details']['state_name'] in flow_stages[wait_stage + 1:]:
                        return "SUCCEEDED"

        # Using warning to silence flow logs
        logging.warning(f'TaskID: {flow_id}, Status: {flow_status}, State: {flow_state}')
        time.sleep(10)


    return flow_status


if __name__ == "__main__":
    main(sys.argv[1:])
