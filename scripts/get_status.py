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


def get_run_url(run_id):
    return f"https://app.globus.org/runs/{run_id}"


if __name__ == "__main__":
    args = arg_parse()
    fc = get_flows_client()

    while True:
        run = fc.get_run(args.run_id)
        if run["status"] in ["SUCCEEDED", "FAILED"]:
            print(f"Run Status: {run['status']}: {get_run_url(args.run_id)}")
            sys.exit(0)

        steps_checked = []
        for pagelog in fc.paginated.get_run_logs(args.run_id, limit=30).items():
            if pagelog.get("code") in ["PassCompleted", "ActionCompleted"]:
                step = pagelog["details"]["state_name"]
                steps_checked.append(step)
                if step == args.step:
                    print(
                        f"Run Status: {run['status']} -- {step}: {pagelog["code"]} {get_run_url(args.run_id)}"
                    )
                    sys.exit(0)

        print(f"Run Status: {run['status']}: {get_run_url(args.run_id)}")
        time.sleep(args.interval)
