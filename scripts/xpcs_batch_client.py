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
from gladier_xpcs.flows.flow_boost_batch import XPCSBoostBatch
from gladier_xpcs.deployments import BaseDeployment, deployment_map

from scripts import xpcs_online_boost_client

SUPPORTED_QUEUES = ["debug", "preemptable", "prod", "demand"]
DEPLOYMENT = deployment_map["voyager-8idi-polaris"]

globus_app = globus_sdk.ClientApp("scripting", client_id=os.getenv("GLADIER_CLIENT_ID"), client_secret=os.getenv("GLADIER_CLIENT_SECRET"))
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)

# To run silently as nick only
# run_kwargs = {"run_monitors": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"],
#               "run_managers": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"]}
run_kwargs = {}

def generate_batches_from_source(
        source: str = "/8IDI/2025-2/tempus202507-merge/data/converted/Ha0277_PO2_a0002_f2000000/",
        qmap: str = "/8IDI/2025-2/tempus202507-merge/data/timepix_Sq90_Dq9_log.hdf",
        staging_dir: str = "batch-test-2025-10-30/tempus202507-merge",
        queue: str = "preemptable",
        batch_size: int = 150,
        limit: int = 0):

    source_path = pathlib.Path(DEPLOYMENT.source_collection.to_globus(source))

    # staging_dir = "batch-test-2025-10-22-batch-2/tempus202507-merge"
    staging_path = pathlib.Path(DEPLOYMENT.get_input()["input"]["staging_dir"]) / staging_dir

    transfer_client = globus_sdk.TransferClient(app=globus_app)
    data = transfer_client.operation_ls(DEPLOYMENT.source_collection.uuid, path=source_path)
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
                "destination_path": str(pathlib.Path(DEPLOYMENT.staging_collection.to_globus(staging_path)) / item / "input"),
                "recursive": True
            }
            for item in file_batch
        ]

        qmap = pathlib.Path(qmap)
        qmap_transfer_item = {
            'destination_path': str(pathlib.Path(DEPLOYMENT.staging_collection.to_globus(staging_path)) / qmap.name),
            'recursive': False,
            'source_path': str(DEPLOYMENT.source_collection.to_globus(str(qmap))),
        }
        transfer_items.append(qmap_transfer_item)

        if queue not in SUPPORTED_QUEUES:
            raise ValueError(f"Invalid Queue {queue}, must be in list: {SUPPPORTED_QUEUES}")

        # See if we can pack 100 datasets into a transfer
        flow_input = {
            "input": {
                "compute_queue": queue,
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


def fetch_dataset_directories(source: str = "/8IDI/2025-2/tempus202507-merge/data/converted/"):
    transfer_client = globus_sdk.TransferClient(app=globus_app)
    source_path = pathlib.Path(DEPLOYMENT.source_collection.to_globus(source))
    data = transfer_client.operation_ls(DEPLOYMENT.source_collection.uuid, path=source_path)
    dataset_directories = [source_path / d["name"] for d in data["DATA"] if d["type"] == "dir"]
    return dataset_directories


@app.command()
def generate_batches(
        limit: int = 0,
        batch_size: int = 200,
        queue: str = "preemptable",
):

    dataset_directories = fetch_dataset_directories()


    experiment = pathlib.Path("tempus202507-merge.json")
    experiment_data = []
    num_datasets = 0

    for d_dir in dataset_directories:
        batches_file = pathlib.Path(d_dir.name)
        d_data = generate_batches_from_source(limit=limit, batch_size=batch_size, queue=queue, source=d_dir)

        experiment_data.append({
            "directory": str(d_dir),
            "batches": d_data,
        })
        num_datasets += sum(b["batch_size"] for b in d_data)

    experiment.write_text(json.dumps(experiment_data, indent=2))
    print(f"Wrote file {experiment}, Datasets: {num_datasets}")


@app.command()
def run_file_batches(
    experiment: str,
    concurrency: int = 5,
    clear: bool = False,
    queue: str = "debug",
):
    experiment = pathlib.Path(experiment)
    e_data = json.loads(experiment.read_text())

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow_client = XPCSBoostBatch(flows_manager=flows_manager)

    current_dataset_directories = []

    for dataset_directory in e_data[0:20]:
        _run_batches(batch_flow_client, dataset_directory["batches"])
        # for batch in dataset_directory["batches"]:
        #     if batch["batch_size"] == 0:
        #         print(f"Batch size for {directory} is zero! Skipping...")
        #         continue

        #     run = _run_batch(batch_flow_client, batch)
        #     batch["run_id"] = run["run_id"]
        #     print(f"Started run with {batch['batch_size']} datasets.")

        current_dataset_directories.append(dataset_directory)
        _wait_for_dataset_directories(batch_flow_client, current_dataset_directories, max_concurrent=5)
    print("Finishing the rest of the datasets")
    _wait_for_dataset_directories(batch_flow_client, current_dataset_directories, max_concurrent=0)
    


def _wait_for_dataset_directories(batch_flow_client, current_dataset_directories, max_concurrent = 0):
    while len(current_dataset_directories) >= max_concurrent and len(current_dataset_directories) != 0:
        done = list()
        for curdir in current_dataset_directories:
            print("Checking dataset directories!")
            if _is_dataset_directory_done(batch_flow_client, curdir):
                done.append(curdir)
        
        for d in done:
            current_dataset_directories.remove(d)
            num_datasets = sum(b["batch_size"] for b in d["batches"])

            print(f"Finished {d['directory']} with {num_datasets} datasets.")

        print(f"Current active dataset directories: {len(current_dataset_directories)}")


def _is_dataset_directory_done(batch_flow_client, dataset_directory):
    for idx, batch in enumerate(dataset_directory["batches"]):
        if batch.get("status") == "ACTIVE":
            print(f"Checking status of batch {idx}/{len(dataset_directory['batches'])} ({dataset_directory['directory']})")
            batch["status"] = batch_flow_client.get_status()

            if batch["status"] == "ACTIVE":
                return False
    return True



def _run_batches(batch_flow_client, batches):

    for idx, batch in enumerate(batches):
        batch["flow_input"]["input"]["compute_queue"] = "debug"

        queue = batch["flow_input"]["input"]["compute_queue"]
        label = f"exp-test-{batch['batch_size']}-{queue}-{idx + 1}-of-{len(batches)}"
        run = batch_flow_client.run_flow(flow_input=batch["flow_input"], label=label, tags=['aps', 'xpcs', 'batch-test', 'test'])
        batch["run_id"] = run["run_id"]

# def get_run_list(session, client):
#     query_params = dict(orderby="start_time DESC", per_page=50)
#     # Fetch recent runs
#     runs = dict()
#     for idx, page in enumerate(client_list_runs(client, query_params)):
#         if idx >= 4:
#             break
#         log.debug(f"Fetching page {idx} of run list...")
#         for run in page["runs"]:
#             runs["run_id"] = run
#     return runs


@app.command()
def run_batches(
        limit: int = 0,
        batch_size: int = 200,
        queue: str = "preemptable",
):

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow = XPCSBoostBatch(flows_manager=flows_manager)

    batches = generate_batches_from_source(limit=limit, batch_size=batch_size, queue=queue)
    for batch in batches:

        # pprint(batch_flow.get_flow_definition())
        # pprint(flow_input)
        queue = batch["flow_input"]["input"]["compute_queue"]
        label = f"exp-test-{batch['batch_size']}-{queue}-{batch['batch_id'] + 1}-of-{len(batches)}"
        run = batch_flow.run_flow(flow_input=batch["flow_input"], label=label, tags=['aps', 'xpcs', 'batch-test'])
        print(run["run_id"])
        # status = batch_flow.progress(run["run_id"])
        # pprint(batch_flow.get_status(run["run_id"]))
        # pprint(status)

if __name__ == "__main__":
    app()