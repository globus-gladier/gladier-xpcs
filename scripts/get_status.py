#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import argparse
import time
import sys

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

    flow_steps = get_ordered_flow_states()
    
    start_time = time.now()


    status = corr_cli.get_status(args.run_id)
    while status['status'] not in ['SUCCEEDED', 'FAILED']:
        if args.timeout:
            cur_time = time.time()
            if int(cur_time - start_time) >= int(args.timeout):
                sys.exit(1)

        status = corr_cli.get_status(args.run_id)
        if status['status'] == 'ACTIVE':
            print(f'[{status["status"]}]: {status["details"]["description"]}')



        # flow_action = corr_cli.flow_action_status(args.flow_id, args.run_id)
        # flow_status = flow_action['status']
        # flow_state = 'DONE'

        # try:
        #     flow_state = flow_action.data['details']['details']['state_name']
        # except Exception as e:
        #     pass

        # if args.step:
        #     flow_log = corr_cli.flow_action_log(args.flow_id, args.run_id)
        #     for l in flow_log['entries']:
        #         if 'details' in l and 'state_name' in l['details']:
        #             # Check if the stage is later than the one we are waiting on
        #             if l['details']['state_name'] in flow_steps[args.step + 1:]:
        #                 sys.exit(0)


        # time.sleep(10)