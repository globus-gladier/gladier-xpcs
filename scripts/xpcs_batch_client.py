import globus_sdk
from pprint import pprint
import urllib
import pathlib
import json
from queue import SimpleQueue
import sys
import functools
import os
import typer
import enum
from typing import Annotated

from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier import FlowsManager
from gladier_xpcs.tools.xpcs_boost_corr import xpcs_boost_corr
from gladier_xpcs.flows.flow_boost_batch import XPCSBoostBatch
from gladier_xpcs.deployments import BaseDeployment, deployment_map

from scripts import xpcs_online_boost_client

DEPLOYMENT = deployment_map["voyager-8idi-polaris"]
SOURCE_ENDPOINT_BASE_PATH = pathlib.Path("/8IDI")

globus_app = globus_sdk.ClientApp("scripting", client_id=os.getenv("GLADIER_CLIENT_ID"), client_secret=os.getenv("GLADIER_CLIENT_SECRET"))
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)

class CorrType(str, enum.Enum):
    Multitau = "Multitau"
    Twotime = "Twotime"
    Both = "Both"


class PolarisQueues(str, enum.Enum):
    debug = "debug"
    preemptable = "preemptable"
    prod = "prod"
    demand = "demand"


PROCESSING_ARGS = dict(
    queue=typer.Option(help="Compute queue to use.", rich_help_panel="Processing Options", show_choices=list(PolarisQueues)),
    group=typer.Option(
            help="Visibility in Search",
            rich_help_panel="Processing Options",
        ),
    deployment=typer.Option(
            help="Deployment configs.",
            rich_help_panel="Processing Options",
            # default="voyager-8idi-polaris",
            show_choices=list(deployment_map.keys()),
        ),
    experiment=typer.Option(
            help="Experiment name for the dataset. Ex: tempus202507-merge",
            rich_help_panel="Processing Options",
        ),
    skip_transfer_back=typer.Option(
        help="Skip transfer of processed data to source collection. "
        "Should not be skipped in normal operation. Use this option only for testing or reprocessing old data.",
        rich_help_panel="Processing Options",
        ),
    output=typer.Option(
        help='This is the "transfer back" output directory on source, where the results corr file will be transferred.',
        rich_help_panel="Processing Options",
    ),
    flow_batch_size=typer.Option(
        help="Number of datasets to process in a single flow batch.",
        rich_help_panel="Processing Options",
    ),
    dataset_limit=typer.Option(
        help="Limit number of datasets to process overall.",
        rich_help_panel="Processing Options",
    ),
)

CORR_ARGS = dict(
    qmap=typer.Option("--qmap", "-q",
            help="Path to the qmap file",
            rich_help_panel="Corr Options",
        ),
    cycle=typer.Option("--cycle", "-c",
            help="cycle for the dataset. Ex: 2025-1. Determines publish location.",
            rich_help_panel="Corr Options",
        ),
    type=typer.Option("--type", "-t",
            help="Analysis type to be performed.",
            rich_help_panel="Corr Options",
        ),
    gpu_id=typer.Option("--gpu-id", "-i",
            help="Choose which GPU to use. if the input is -1, then CPU is used",
            rich_help_panel="Corr Options",
        ),
    batch_size=typer.Option(
            help="Size of gpu corr processing batch",
            rich_help_panel="Corr Options",
        ),
    verbose=typer.Option("--verbose", "-v",
            help="Verbose output",
            rich_help_panel="Corr Options",
        ),
    smooth=typer.Option("--smooth", "-s",
        help="Smooth method to be used in Twotime correlation.",
        rich_help_panel="Corr Options",
        ),
    save_g2=typer.Option("--save-g2", "-G",
            help="Save G2, IP, and IF to file.",
            rich_help_panel="Corr Options",
        ),
    avg_frame=typer.Option("--avg-frame", "-a",
        help="Defines the number of frames to be averaged before the correlation.",
        rich_help_panel="Corr Options",
        ),
    begin_frame=typer.Option("--begin-frame", "-b",
            help="Specifies which frame to begin with for the correlation. ",
            rich_help_panel="Corr Options",
        ),
    end_frame=typer.Option("--end-frame", "-e",
        help="Specifies the last frame used for the correlation.",
        rich_help_panel="Corr Options",
        ),
    stride_frame=typer.Option("--stride-frame", "-f",
        help="Defines the stride.",
        rich_help_panel="Corr Options",
        ),
    overwrite=typer.Option("--overwrite", "-w",
        help="Overwrite the existing result file.",
        rich_help_panel="Corr Options",
        ),
    dq_selection=typer.Option("--dq-selection", "-d",
        help="A string that selects the dq list, eg. '1, 2, 5-7' selects [1,2,5,6,7]",
        rich_help_panel="Corr Options",
        ),
)

# To run silently as nick only
# run_kwargs = {"run_monitors": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"],
#               "run_managers": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"]}
run_kwargs = {}


def get_flow_batch_input(
        source_files: str,
        qmap: str,
        staging_dir: str,
        queue: str = "preemptable",
        flow_batch_size: int = 150,
        boost_corr_args: dict = None,):

    """
    Get a list of batches to process from source files. Each batch contains flow input for starting a flow.

    Args:
        source_files (str): List of source file paths to process. Each directory must contain data files.
            Example: ["/8IDI/2025-2/tempus202507-merge/data/converted/Ha0277_PO2_a0002_f2000000/Eb0082_D20_a0011_f2000000_r00001_t76ns/", ...]
        qmap (str): Path to the qmap file.
            Example: "/8IDI/2025-2/tempus202507-merge/data/timepix_Sq90_Dq9_log.hdf"
        staging_dir (str): Staging directory path.
            Example: "2025-2/tempus202507-merge"
        queue (str): Compute queue to use.
            Example: "preemptable"
        flow_batch_size (int): Number of datasets per flow batch.
        boost_corr_args (dict): Arguments for boost_corr tool. See CORR_ARGS for details.
    """

    if not source_files:
        return list()

    source_base = pathlib.Path(source_files[0]).parent
    source_paths = [pathlib.Path(DEPLOYMENT.source_collection.to_globus(f)) for f in source_files]
    staging_path = pathlib.Path(DEPLOYMENT.get_input()["input"]["staging_dir"]) / staging_dir

    # Chunk the files into batches, using relative paths from source base
    files = [f.relative_to(source_base) for f in source_paths]
    batches = [list(files[i: i + flow_batch_size]) for i in range(0, len(files), flow_batch_size)]

    finalized_batches = list()
    for idx, file_batch in enumerate(batches):

        transfer_items = [
            {
                "source_path": str(source_base / item),
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

        # See if we can pack 100 datasets into a transfer
        flow_input = {
            "input": {
                "compute_queue": queue,
                "compute_endpoint": "d88919ea-026a-493e-9124-fe3c46defa54",
                "staging_base_path": str(staging_path),
                "staging_qmap": qmap_transfer_item["destination_path"],
                # "boost_corr": get_boost_corr_defaults(),
                "boost_corr": boost_corr_args,
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
            "run_id": None,
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


def get_experiment_and_cycle_from_path(path: str):
    path_parts = pathlib.Path(path).relative_to(SOURCE_ENDPOINT_BASE_PATH).parts
    try:
        cycle = path_parts[0]
        print("parsing cycle:", cycle)
        year, trimester = cycle.split('-')
        if not int(year) in range(2015, 2050) or not int(trimester) in range(1, 4):
            raise ValueError(f"Invalid cycle {cycle} from path {path}")
    except Exception as e:
        print(e)
        raise ValueError(f"Failed to parse cycle from path {path}")
    experiment = path_parts[1]
    return experiment, cycle


def fetch_source_directory_metadata(
    path: str, collection_uuid: str = None, filter_type: str = "dir"
):
    collection_uuid = collection_uuid or DEPLOYMENT.source_collection.uuid
    transfer_client = globus_sdk.TransferClient(app=globus_app)
    source_path = pathlib.Path(DEPLOYMENT.source_collection.to_globus(path))
    data = transfer_client.operation_ls(collection_uuid, path=source_path)
    return [f for f in data["DATA"] if filter_type is None or f["type"] == filter_type]


def fetch_source_directory(
    path: str, collection_uuid: str = None, filter_type: str = "dir"
):
    source_path = pathlib.Path(DEPLOYMENT.source_collection.to_globus(path))
    return [source_path/d["name"] for d in fetch_source_directory_metadata(path, collection_uuid, filter_type)]


@app.command()
def list_cycles():
    print(f"Listing Cycles for endpoint {DEPLOYMENT.source_collection.uuid}, path {SOURCE_ENDPOINT_BASE_PATH}:")
    for cycle in fetch_source_directory(path=SOURCE_ENDPOINT_BASE_PATH):
        print(f" - {cycle}")


@app.command()
def list_experiments(cycle: str = "2025-2"):
    path = SOURCE_ENDPOINT_BASE_PATH / cycle
    print(f"Listing Experiments in {DEPLOYMENT.source_collection.uuid}{path}:")
    for exp in fetch_source_directory(path=path):
        print(f" - {exp}")


@app.command()
def list_qmaps(
        path: str = None,
        experiment: str = None,
        cycle: str = "2025-2",
):
    if not path and (not experiment or not cycle):
        print("Must provide either path or experiment and cycle!")
        return
    path = pathlib.Path(path) or SOURCE_ENDPOINT_BASE_PATH / cycle / experiment / "data"
    print(f"Listing Qmaps in {DEPLOYMENT.source_collection.uuid}{path}:")
    qmaps = fetch_source_directory_metadata(path=path, filter_type="file")
    qmaps = [q for q in qmaps if q["name"].endswith(".hdf")]
    qmaps.sort(key=lambda x: x["last_modified"])
    for qmap in qmaps:
        print(f"    - {path / qmap['name']} (last modified: {qmap['last_modified']})")


@app.command()
def list_experiment_subdirectories(
        experiment: str = None,
        cycle: str = "2025-2",
        count_subfolders: bool = False,
        path: str = None,
):
    if not experiment and not path:
        print("Must provide either experiment or path!")
        return

    if path:
        path = pathlib.Path(path)
        experiment_parts = path.relative_to(SOURCE_ENDPOINT_BASE_PATH).parts
        cycle = experiment_parts[0]
        experiment = experiment_parts[1]
    else:
        path = SOURCE_ENDPOINT_BASE_PATH / cycle / experiment / "data"

    csv_path = pathlib.Path(f"{cycle}_{experiment}_subfolder_counts.csv")
    if csv_path.exists():
        print(csv_path.read_text())
        return
    print(
        f"Listing Experiment Subdirectories in {DEPLOYMENT.source_collection.uuid}{path}:"
    )
    counted_subfolders = dict()

    for ddir in fetch_source_directory(path=path):
        if count_subfolders:
            subfolders = len(
                list(fetch_source_directory(path=path / ddir, filter_type="dir"))
            )
            counted_subfolders[str(path / ddir)] = subfolders
            print(f" - {ddir} (subfolders: {subfolders})")
        else:
            print(f" - {ddir}")

    if count_subfolders:
        csv = "path,subfolder_count\n"
        for k, v in counted_subfolders.items():
            csv += f"{k},{v}\n"
        csv_path.write_text(csv)
        print(f"Wrote subfolder counts to {csv_path}")


@app.command()
def run_experiment_subdirectory(
    path: str,
    qmap: Annotated[str, CORR_ARGS["qmap"]],
    cycle: Annotated[str, CORR_ARGS["cycle"]] = None,
    experiment: Annotated[str, PROCESSING_ARGS["experiment"]] = None,
    deployment: Annotated[str, PROCESSING_ARGS["deployment"]] = "voyager-8idi-polaris",
    group: Annotated[str, PROCESSING_ARGS["group"]] = "368beb47-c9c5-11e9-b455-0efb3ba9a670",
    queue: Annotated[PolarisQueues, PROCESSING_ARGS["queue"]] = PolarisQueues.preemptable,
    flow_batch_size: Annotated[int, PROCESSING_ARGS["flow_batch_size"]] = 200,
    dataset_limit: Annotated[int, PROCESSING_ARGS["dataset_limit"]] = 0,
    type: Annotated[CorrType, CORR_ARGS["type"]] = CorrType.Multitau,
    gpu_id: Annotated[int, CORR_ARGS["gpu_id"]] = 0,
    batch_size: Annotated[int, CORR_ARGS["batch_size"]] = 256,
    verbose: Annotated[bool, CORR_ARGS["verbose"]] = True,
    smooth: Annotated[str, CORR_ARGS["smooth"]] = "sqmap",
    save_g2: Annotated[bool, CORR_ARGS["save_g2"]] = False,
    avg_frame: Annotated[int, CORR_ARGS["avg_frame"]] = 1,
    begin_frame: Annotated[int, CORR_ARGS["begin_frame"]] = 0,
    end_frame: Annotated[int, CORR_ARGS["end_frame"]] = -1,
    stride_frame: Annotated[int, CORR_ARGS["stride_frame"]] = 1,
    overwrite: Annotated[bool, CORR_ARGS["overwrite"]] = True,
    dq_selection: Annotated[str, CORR_ARGS["dq_selection"]] = "all",
):
    if not experiment or not cycle:
        try:
            experiment, cycle = get_experiment_and_cycle_from_path(path)
        except ValueError as e:
            print(str(e))
            print("Must provide experiment and cycle if they cannot be parsed from path!")

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow_client = XPCSBoostBatch(flows_manager=flows_manager)

    source_files = fetch_source_directory(path=path, filter_type="dir")
    if dataset_limit > 0:
        source_files = source_files[:dataset_limit]

    batches = get_flow_batch_input(
        source_files=source_files,
        qmap=qmap,
        staging_dir=f"batch-test-2026-01-27-{cycle}-{experiment}",
        queue=queue.value,
        flow_batch_size=flow_batch_size,
        boost_corr_args=dict(
            type=type.value,
            gpu_id=gpu_id,
            batch_size=batch_size,
            verbose=verbose,
            smooth=smooth,
            save_g2=save_g2,
            avg_frame=avg_frame,
            begin_frame=begin_frame,
            end_frame=end_frame,
            stride_frame=stride_frame,
            overwrite=overwrite,
            dq_selection=dq_selection,
        ),
    )

    for idx, batch in enumerate(batches):
        label = f"exp-test-{batch['batch_size']}-{queue.value}-{idx + 1}-of-{len(batches)}"
        from pprint import pprint
        pprint(batch["flow_input"])
        run = batch_flow_client.run_flow(flow_input=batch["flow_input"], label=label, tags=['aps', 'xpcs', 'batch-test', 'test'])
        batch["run_id"] = run["run_id"]

        print(f"Started run with {batch['batch_size']} datasets. https://app.globus.org/runs/{run['run_id']}/logs")


def get_experiment_manifest(
    experiment_data_path: pathlib.Path,
    qmap: str,
    cycle: str,
    experiment: str,
    deployment: str,
    group: str,
    queue: str,
    flow_batch_size: int,
    dataset_limit: int,
    boost_corr_args: dict,
):
    manifest_file = pathlib.Path(f"manifest-{cycle}-{experiment}.json")
    if manifest_file.exists():
        print(f"Loading existing manifest from {manifest_file}")
        return json.loads(manifest_file.read_text())

    manifest = dict(
            path=str(experiment_data_path),
            experiment=experiment,
            cycle=cycle,
            experiment_subdirectories={},
            deployment=deployment,
            group=group,
            dataset_limit=dataset_limit,
            queue=queue,
            flow_batch_size=flow_batch_size,
            boost_corr_args=boost_corr_args,
            filename=str(manifest_file),
        )
    for exp_subdir in fetch_source_directory(path=experiment_data_path, filter_type="dir"):
        print(f"Processing subdirectory {exp_subdir}")
        batches = generate_batches_from_source(
            source=str(experiment_data_path / exp_subdir),
            qmap=qmap,
            queue=queue,
            flow_batch_size=flow_batch_size,
            dataset_limit=dataset_limit,
            boost_corr_args=boost_corr_args,
        )
        manifest["experiment_subdirectories"][str(experiment_data_path / exp_subdir)] = batches
    manifest_file.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote manifest to {manifest_file}")
    return manifest

def get_run_list(session, client):
    flows_client = globus_sdk.FlowsClient(app=globus_app)
    query_params = dict(orderby="start_time DESC", per_page=50)
    runs = {}
    # Fetch recent runs
    for idx, page in enumerate(flows_client.paginated.list_runs(query_params=query_params)):
        if idx >= 4:
            break
        # print(f"Fetching page {idx} of run list...")
        for run in page["runs"]:
            runs[run["run_id"]] = run
    return runs


def update_manifest(manifest: dict, updates: dict = None):
    run_list = get_run_list(globus_app, globus_sdk.FlowsClient)

    updates = updates or dict()
    active_runs = 0
    for exp_subdir, batches in manifest["experiment_subdirectories"].items():
        for idx, batch in enumerate(batches):
            # Update run ID if provided
            new_run = updates.get((exp_subdir, idx))
            if new_run:
                batch["run_id"] = new_run

            # Update run status
            run_id = batch.get("run_id")
            if run_id and run_id not in run_list:
                raise Exception(f"Run ID {run_id} not found in recent runs!")
            elif run_id:
                batch["run_status"] = run_list[run_id]["status"]
                active_runs += 1 if batch["run_status"] == "ACTIVE" else 0
    manifest_file = pathlib.Path(manifest["filename"])
    manifest_file.write_text(json.dumps(manifest, indent=2))
    return active_runs
            

@app.command()
def run_experiment(
        path: str,
        qmap: Annotated[str, CORR_ARGS["qmap"]],
        cycle: Annotated[str, CORR_ARGS["cycle"]] = None,
        experiment: Annotated[str, PROCESSING_ARGS["experiment"]] = None,
        deployment: Annotated[str, PROCESSING_ARGS["deployment"]] = "voyager-8idi-polaris",
        group: Annotated[str, PROCESSING_ARGS["group"]] = "368beb47-c9c5-11e9-b455-0efb3ba9a670",
        queue: Annotated[PolarisQueues, PROCESSING_ARGS["queue"]] = PolarisQueues.preemptable,
        flow_batch_size: Annotated[int, PROCESSING_ARGS["flow_batch_size"]] = 200,
        dataset_limit: Annotated[int, PROCESSING_ARGS["dataset_limit"]] = 0,
        type: Annotated[CorrType, CORR_ARGS["type"]] = CorrType.Multitau,
        gpu_id: Annotated[int, CORR_ARGS["gpu_id"]] = 0,
        batch_size: Annotated[int, CORR_ARGS["batch_size"]] = 256,
        verbose: Annotated[bool, CORR_ARGS["verbose"]] = True,
        smooth: Annotated[str, CORR_ARGS["smooth"]] = "sqmap",
        save_g2: Annotated[bool, CORR_ARGS["save_g2"]] = False,
        avg_frame: Annotated[int, CORR_ARGS["avg_frame"]] = 1,
        begin_frame: Annotated[int, CORR_ARGS["begin_frame"]] = 0,
        end_frame: Annotated[int, CORR_ARGS["end_frame"]] = -1,
        stride_frame: Annotated[int, CORR_ARGS["stride_frame"]] = 1,
        overwrite: Annotated[bool, CORR_ARGS["overwrite"]] = True,
        dq_selection: Annotated[str, CORR_ARGS["dq_selection"]] = "all",
    ):
        if not experiment or not cycle:
            try:
                experiment, cycle = get_experiment_and_cycle_from_path(path)
            except ValueError as e:
                print(str(e))
                print("Must provide experiment and cycle if they cannot be parsed from path!")
                return

        manifest = get_experiment_manifest(
            experiment_data_path=pathlib.Path(path),
            qmap=qmap,
            cycle=cycle,
            experiment=experiment,
            deployment=deployment,
            group=group,
            queue=queue.value,
            flow_batch_size=flow_batch_size,
            dataset_limit=dataset_limit,
            boost_corr_args=dict(
                type=type.value,
                gpu_id=gpu_id,
                batch_size=batch_size,
                verbose=verbose,
                smooth=smooth,
                save_g2=save_g2,
                avg_frame=avg_frame,
                begin_frame=begin_frame,
                end_frame=end_frame,
                stride_frame=stride_frame,
                overwrite=overwrite,
                dq_selection=dq_selection,
            ),
        )

        work_queue = SimpleQueue()
        for exp_subdir, batches in manifest["experiment_subdirectories"].items():
            for idx, batch in enumerate(batches):
                if batch.get("run_id"):
                    print(f"Batch {idx + 1}/{len(batches)} in {exp_subdir} already has run ID {batch['run_id']}. Skipping...")
                    continue

                work_item = {
                    "experiment_subdirectory": exp_subdir,
                    "batch_index": idx,
                    "flow_data": {
                        "flow_input": batch["flow_input"],
                        "label": f"exp-test-{batch['batch_size']}-{queue.value}-{idx + 1}-of-{len(batches)}",
                        "tags": ['aps', 'xpcs', 'batch-test', 'test'],
                    }
                }
                work_queue.put(work_item)

        flows_manager = FlowsManager(run_kwargs=run_kwargs)
        batch_flow_client = XPCSBoostBatch(flows_manager=flows_manager)
        runs_limit = 1
        active_runs = update_manifest(manifest)
        initial_queue_size = work_queue.qsize()
        print(f"Starting processing of {initial_queue_size} batches with max concurrency {runs_limit}...")
        while not work_queue.empty():
            num_runs_to_start = runs_limit - active_runs
            run_updates = {}
            for _ in range(num_runs_to_start):
                work_item = work_queue.get()
                run = batch_flow_client.run_flow(**work_item["flow_data"])
                print(f"Started {work_item['flow_data']['label']} with run ID {run['run_id']}")
                run_updates[(work_item["experiment_subdirectory"], work_item["batch_index"])] = run["run_id"]
            active_runs = update_manifest(manifest, run_updates)
            print(f"Active runs: {active_runs}, Batches remaining: {work_queue.qsize()}/{initial_queue_size}")


if __name__ == "__main__":
    try:
        app()
    except globus_sdk.TransferAPIError as e:
        print(f"Globus Transfer API Error: {e.message}")
        sys.exit(1)
