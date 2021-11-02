#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import argparse
import time
import sys
from pprint import pprint

from gladier.utils.flow_generation import get_ordered_flow_states

##import the client
from gladier_xpcs.flow_online import XPCSOnlineFlow
def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", help="The automate flow instance(run) to check.",
                        default=None)
    parser.add_argument("--step", help="The inside the flow execution to check",
                        default=None)
    parser.add_argument("--timeout", help="How long to wait before exiting.",
                        default=300)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = arg_parse()


    mainFlow = XPCSOnlineFlow()

    flow_dict = get_ordered_flow_states(mainFlow.flow_definition)
    flow_steps = []
    for key, value in flow_dict.items() :
        flow_steps.append(key)
    
    start_time = time.time()

    if args.step not in flow_steps:
        print(args.step + ' not in valid steps')
        print(flow_steps)
        sys.exit(-1)


    step_index = flow_steps.index(args.step)

    status = mainFlow.get_status(args.run_id)

    while status['status'] not in ['SUCCEEDED', 'FAILED']:

        if args.timeout:
            cur_time = time.time()
            if int(cur_time - start_time) >= int(args.timeout):
                sys.exit(1)

        status = mainFlow.get_status(args.run_id)

        if status.get('state_name'):
            curr_step = status.get('state_name')       
        elif status.get('details'):
            det = status.get('details')
            if det.get('details'):
                curr_step = status['details']['details']['state_name']
            elif det.get('action_statuses'):
                curr_step = status['details']['action_statuses'][0]['state_name']
        
        curr_index = flow_steps.index(curr_step)

        if status['status']=='FAILED': #this could be out of the loop to prevent overchecking
            sys.exit(1)

        if curr_index>step_index:
            sys.exit(0)

        time.sleep(90)

