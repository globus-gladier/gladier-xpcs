#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python
import argparse
import os
import sys
import time
import globus_sdk

# Get client id/secret
CLIENT_ID = os.getenv("GLADIER_CLIENT_ID")
CLIENT_SECRET = os.getenv("GLADIER_CLIENT_SECRET")


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run_id", help="The automate flow instance(run) to check.", required=True
    )
    parser.add_argument(
        "--step",
        help="The inside the flow execution to check",
        default="ResultTransferDone",
    )
    parser.add_argument(
        "--interval", help="Interval between checking statuses", type=int, default=5
    )
    parser.add_argument(
        "--max-wait",
        help="Maixmum time to wait for a run before exiting",
        type=int,
        default=60 * 60,
    )
    args = parser.parse_args()
    return args


def get_flows_client():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise Exception(
            "No client credentials specified. Please set these env vars: GLADIER_CLIENT_ID, GLADIER_CLIENT_SECRET"
        )
    app = globus_sdk.ClientApp(
        app_name="XPCSBoost",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )
    return globus_sdk.FlowsClient(app=app)


def get_run_url(run_id: str):
    return f"https://app.globus.org/runs/{run_id}"


def get_run_logs(flows_client: globus_sdk.FlowsClient, run_id: str):
    return flows_client.paginated.get_run_logs(run_id, limit=30).items()


def is_target_state_in_run_logs(
    flows_client: globus_sdk.FlowsClient, run_id: str, target_step: str
):
    for pagelog in get_run_logs(flows_client, run_id):
        if pagelog.get("code") in ["PassCompleted", "ActionCompleted"]:
            step = pagelog["details"]["state_name"]
            if step == target_step:
                return True
    return False


def main_loop(args: argparse.ArgumentParser, flows_client: globus_sdk.FlowsClient):
    start_time = time.time()
    elapsed_time = 0

    while elapsed_time < args.max_wait:
        run = flows_client.get_run(args.run_id)
        if run["status"] in ["SUCCEEDED", "FAILED"]:
            print(
                f"Run Status: {run['status']} ({elapsed_time}/{args.max_wait}): {get_run_url(args.run_id)}"
            )
            sys.exit(0)

        if is_target_state_in_run_logs(flows_client, run["run_id"], args.step) is True:
            print(
                f"Run Status: {run['status']} ({elapsed_time}/{args.max_wait}): -- {args.step}: Completed {get_run_url(args.run_id)}"
            )
            sys.exit(0)

        print(
            f"Run Status: {run['status']} ({elapsed_time}/{args.max_wait}): {get_run_url(args.run_id)}"
        )
        time.sleep(args.interval)
        elapsed_time = int(time.time() - start_time)


if __name__ == "__main__":
    main_loop(arg_parse(), get_flows_client())
