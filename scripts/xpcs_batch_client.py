import globus_sdk
from pprint import pprint
import urllib
import pathlib
import json
import sys
import functools
import os
import typer

from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier import FlowsManager
from gladier_xpcs.tools.xpcs_boost_corr import xpcs_boost_corr
from gladier_xpcs.flows.flow_boost_batch import XPCSBoostBatch, QUEUE
from gladier_xpcs.deployments import BaseDeployment, deployment_map

from scripts import xpcs_online_boost_client

globus_app = globus_sdk.ClientApp("scripting", client_id=os.getenv("GLADIER_CLIENT_ID"), client_secret=os.getenv("GLADIER_CLIENT_SECRET"))
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)

# To run silently as nick only
# run_kwargs = {"run_monitors": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"],
#               "run_managers": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"]}
run_kwargs = {}

def generate_batches_from_source(
        source: str = "/8IDI/2025-2/tempus202507-merge/data/converted/Ha0277_PO2_a0002_f2000000/",
        qmap: str = "/8IDI/2025-2/tempus202507-merge/data/timepix_Sq90_Dq9_log.hdf",
        staging_dir: str = "batch-test-2025-10-22-batch-2/tempus202507-merge",
        batch_size: int = 100,
        limit: int = 0):

    # Voyager
    deployment = deployment_map["voyager-8idi-polaris"]
    source_path = pathlib.Path(deployment.source_collection.to_globus(source))

    # staging_dir = "batch-test-2025-10-22-batch-2/tempus202507-merge"
    staging_path = pathlib.Path(deployment.get_input()["input"]["staging_dir"]) / staging_dir

    transfer_client = globus_sdk.TransferClient(app=globus_app)
    data = transfer_client.operation_ls(deployment.source_collection.uuid, path=source_path)
    files = [d["name"] for d in data["DATA"] if d["type"] == "dir"]
    if limit:
        files = files[0:limit]
        print(f"WARNING: Capped total amount of files to {limit}")
    batches = [list(files[i: i + batch_size]) for i in range(0, len(files), batch_size)]

    finalized_batches = list()
    for idx, file_batch in enumerate(batches):

        transfer_items = [
            {
                "source_path": str(source_path / item),
                "destination_path": str(pathlib.Path(deployment.staging_collection.to_globus(staging_path)) / item / "input"),
                "recursive": True
            }
            for item in file_batch
        ]

        qmap = pathlib.Path(qmap)
        qmap_transfer_item = {
            'destination_path': str(pathlib.Path(deployment.staging_collection.to_globus(staging_path)) / qmap.name),
            'recursive': False,
            'source_path': str(deployment.source_collection.to_globus(str(qmap))),
        }
        transfer_items.append(qmap_transfer_item)

        # See if we can pack 100 datasets into a transfer
        flow_input = {
            "input": {
                "compute_endpoint": "d88919ea-026a-493e-9124-fe3c46defa54",
                "staging_base_path": str(staging_path),
                "staging_qmap": qmap_transfer_item["destination_path"],
                "boost_corr": get_boost_corr_defaults(),
                "qmap": "/eagle/APSDataProcessing/aps8idi" + qmap_transfer_item["destination_path"],
                "xpcs_boost_corr_tasks": [
                    {
                        "function_id": get_function_id("xpcs_boost_corr_function_id"),
                        'kwargs': {'corr_input_file': "/eagle/APSDataProcessing/aps8idi/" + d["destination_path"]},
                    } for d in transfer_items[:-1]
                ],
                'source_transfer': {
                    'destination_endpoint_id': '98d26f35-e5d5-4edd-becf-a75520656c64',
                    'source_endpoint_id': 'aa2b18e8-e248-4265-985c-7e2e59765539',
                    'transfer_items': transfer_items
                },
            },
        }

        batch = {
            "flow_id": None,
            "flow_input": flow_input,
            "batch_id": idx,
            "batch_size": len(file_batch),
        }
        finalized_batches.append(batch)
    return finalized_batches


@functools.lru_cache()
def get_function_id(name: str):
    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow = XPCSBoostBatch(flows_manager=flows_manager)
    fids = batch_flow.get_compute_function_ids()
    if not fids.get("xpcs_boost_corr_function_id"):
        print("WARNING: No xpcs_boost_corr_function_id function found!")
    return fids[name]



def get_boost_corr_defaults():
    """
    Boost Corr Arguments mostly come from the input section, and is something we can't easily mock.

    The main things we overwrite from these values are 'raw', 'qmap', and 'type'. Output typically isn't
    used since we don't want to transfer these back to Voyager.

    Other values we don't tend to change.
    """
    return {
        "avg_frame": 1,
        "begin_frame": 0,
        "dq_selection": "all",
        "end_frame": -1,
        "overwrite": True,
        # "qmap": "",
        # "raw": "",
        "save_g2": False,
        "smooth": "sqmap",
        "stride_frame": 1,
        "verbose": True,

        "type": "Multitau",
        "gpu_id": 0,
        # "output": "",
    }

@app.command()
def generate_batches(
        limit: int = 0,
        batch_size: int = 200,
):
    batches = generate_batches_from_source(limit=10, batch_size=1)
    from pprint import pprint
    pprint(batches)


@app.command()
def run_batches(
        limit: int = 0,
        batch_size: int = 200,
):

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow = XPCSBoostBatch(flows_manager=flows_manager)

    batches = generate_batches_from_source(limit=limit, batch_size=batch_size)
    for batch in batches:

        # pprint(batch_flow.get_flow_definition())
        # pprint(flow_input)
        label = f"exp-test-{batch['batch_size']}-{QUEUE}-{batch['batch_id'] + 1}-of-{len(batches)}"
        run = batch_flow.run_flow(flow_input=batch["flow_input"], label=label, tags=['aps', 'xpcs', 'batch-test'])
        print(run["run_id"])
        # status = batch_flow.progress(run["run_id"])
        # pprint(batch_flow.get_status(run["run_id"]))
        # pprint(status)

if __name__ == "__main__":
    app()