#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

from gladier import GladierBaseClient
import argparse

class checkClient(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--flow_id", help="The automate flow to check.",
                        default=None)
    parser.add_argument("--run_id", help="The automate flow instance(run) to check.",
                        default=None)
    parser.add_argument("--step", help="The inside the flow execution to check",
                        default=None)
    parser.add_argument("--timeout", help="How long to wait before exiting.",
                        default=None)
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = arg_parse()

    client = checkClient()
    client.get_status(args.task_id)


#     while flow_status == 'ACTIVE':
#         # Break if the walltime has passed
#         if walltime:
#             cur_time = time.time()
#             if int(cur_time - start_time) >= int(walltime):
#                 return "FAILED"

#         flow_action = flows_client.flow_action_status(flow_id, flow_scope, flow_action_id)
#         flow_status = flow_action['status']
#         flow_state = 'DONE'

#         try:
#             flow_state = flow_action.data['details']['details']['state_name']
#         except Exception as e:
#             pass

#         if wait_step:
#             flow_log = flows_client.flow_action_log(flow_id, flow_scope, flow_action_id)
#             for l in flow_log['entries']:
#                 if 'details' in l and 'state_name' in l['details']:
#                     # Check if the stage is later than the one we are waiting on
#                     if l['details']['state_name'] in flow_stages[wait_stage + 1:]:
#                         return "SUCCEEDED"

#         # Using warning to silence flow logs
#         logging.warning(f'TaskID: {flow_id}, Status: {flow_status}, State: {flow_state}')
#         time.sleep(10)


#     return flow_status

    flow_response = 0
    if flow_response == "SUCCEEDED":
        sys.exit(0)
    else:
        sys.exit(1)