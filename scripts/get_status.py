#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import argparse
import time
import sys
import traceback
import pprint

from gladier.utils.flow_generation import get_ordered_flow_states
from gladier_xpcs.flow_online import XPCSOnlineFlow


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", help="The automate flow instance(run) to check.",
                        default=None)
    parser.add_argument("--step", help="The inside the flow execution to check",
                        default=None)
    parser.add_argument("--timeout", help="How long to wait before exiting. Defaults to "
                                          "the original flow state or 300.",
                        default=0)
    parser.add_argument("--interval", help="Interval between checking statuses", type=int,
                        default=90)
    args = parser.parse_args()
    return args


class TimedOut(Exception):
    pass


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
                return list(det['details']['output'].values())[0]['state_name']
            elif det.get('action_statuses'):
                return run_status['details']['action_statuses'][0]['state_name']
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


def check_time(start_time, limit):
    if time.time() - start_time >= float(limit):
        raise TimedOut(f'Wait time has exceeded its limit of {limit} seconds.')


if __name__ == '__main__':
    args = arg_parse()
    main_flow = XPCSOnlineFlow()
    flow_states = list(get_ordered_flow_states(main_flow.flow_definition).keys())
    start_time = time.time()

    if args.step not in flow_states:
        print(f'"{args.step}" is not valid, please choose from: {", ".join(flow_states)}')
        sys.exit(-1)

    state_wait_time = main_flow.flow_definition['States'][args.step].get('WaitTime')
    timeout = args.timeout or state_wait_time or 300

    try:
        while True:
            status = main_flow.get_status(args.run_id)
            check_time(start_time, timeout)
            check_run_status(status)
            if state_has_completed(args.step, flow_states, status):
                print(f'Step {args.step}: Completed')
                sys.exit(0)
            time.sleep(args.interval)
    except RunSucceeded as rs:
        print(str(rs))
        sys.exit(0)
    except (TimedOut, RunFailed) as e:
        print(f'{e.__class__.__name__}: {str(e)}', file=sys.stderr)
        sys.exit(1)
