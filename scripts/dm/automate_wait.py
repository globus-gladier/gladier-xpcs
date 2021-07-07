#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

from gladier import GladierBaseClient

import argparse
from globus_automate_client import create_flows_client

class XPCSClient(GladierBaseClient):
    globus_group = '368beb47-c9c5-11e9-b455-0efb3ba9a670'

def main(arg):
    """
    Wait for the task id to complete
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id", help="The automate flow to process the data.",
                        default=None)
    parser.add_argument("--flow_id", help="The automate flow to process the data.",
                        default=None)
    parser.add_argument("--flow_scope", help="The automate flow scope to use to process the data.",
                        default=None)
    parser.add_argument("--step", help="The step to wait on. Options: Transfer1, ExecCorr, Transfer2, ExecPlots, ExecPilot",
                        default=None)
    parser.add_argument("--walltime", help="How long to wait before exiting.",
                        default=None)
    args = parser.parse_args()

    flows_client = XPCSClient()

#    flow_response = automate_wait(flows_client, args.flow_id, args.flow_scope, args.task_id, args.step, args.walltime)
    flow_response = 0
    if flow_response == "SUCCEEDED":
        sys.exit(0)
    else:
        sys.exit(1)



# def automate_wait(flows_client, flow_id, flow_scope, flow_action_id, wait_step, walltime):
#     """
#     Initiate the flow.
#     """


#     #  corr_cli = XPCSClient()
#     # pprint.pprint(corr_cli.flow_definition)
#     # corr_flow = corr_cli.run_flow(flow_input=flow_input, flow_label='foo')
#     # corr_cli.progress(corr_flow['action_id'])
#     # pprint.pprint(corr_cli.get_status(corr_flow['action_id']))


#     xpcs_client = XPCSClient()
#     if not flow_id:
#         flow_id = xpcs_client.auto_flowid
#         flow_scope = xpcs_client.auto_scope
#     start_time = time.time()
#     flow_status = 'ACTIVE'
#     # Define the stages so we can check if the --step field has finished
#     flow_stages = ['Transfer1', 'ExecCorr', 'Transfer2', 'ExecPlots', 'ExecPilot', 'End']
#     wait_stage = None
#     if wait_step:
#         wait_stage = flow_stages.index(wait_step)

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


if __name__ == "__main__":
    main(sys.argv[1:])
