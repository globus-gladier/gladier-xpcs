#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import argparse
import time
import sys
from pprint import pprint

from gladier.utils.flow_generation import get_ordered_flow_states

##import the client
from gladier_xpcs.online_processing import XPCSClient
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


    corr_cli = XPCSClient()

    flow_dict = get_ordered_flow_states(corr_cli.flow_definition)
    flow_steps = []
    for key, value in flow_dict.items() :
        flow_steps.append(key)
    
    start_time = time.time()

    if args.step not in flow_steps:
        print(args.step + ' not in valid steps')
        print(flow_steps)
        sys.exit(-1)


    step_index = flow_steps.index(args.step)

    status = corr_cli.get_status(args.run_id)

    while status['status'] not in ['SUCCEEDED', 'FAILED']:

        if args.timeout:
            cur_time = time.time()
            if int(cur_time - start_time) >= int(args.timeout):
                print('Out of Time!!')
                sys.exit(1)

        status = corr_cli.get_status(args.run_id)
        curr_step = status['details']['action_statuses'][0]['state_name']
        curr_index = flow_steps.index(curr_step)

        if curr_index<step_index:
            print('I am in the past')
        if curr_index==step_index:
            print('I am in the present')
        if curr_index>step_index:
            print('I am in the future')

        time.sleep(2)


