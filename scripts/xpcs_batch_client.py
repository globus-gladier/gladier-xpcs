import globus_sdk
import datetime
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
import xpcs_online_boost_client

DEPLOYMENT = deployment_map["voyager-8idi-polaris"]
SOURCE_ENDPOINT_BASE_PATH = pathlib.Path("/8IDI")

globus_app = globus_sdk.ClientApp(
    "scripting",
    client_id=os.getenv("GLADIER_CLIENT_ID"),
    client_secret=os.getenv("GLADIER_CLIENT_SECRET"),
)
app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)


class CorrType(str, enum.Enum):
    Multitau = "Multitau"
    Twotime = "Twotime"
    Both = "Both"


class PolarisQueues(str, enum.Enum):
    debug = "debug"
    debug_scaling = "debug-scaling"
    preemptable = "preemptable"
    prod = "prod"
    demand = "demand"


PROCESSING_ARGS = dict(
    queue=typer.Option(
        help="Compute queue to use.",
        rich_help_panel="Processing Options",
        show_choices=list(PolarisQueues),
    ),
    walltime=typer.Option(
        help="Walltime for compute jobs (HH:MM:SS).",
        rich_help_panel="Processing Options",
    ),
    nodes_per_block=typer.Option(
        help="Number of nodes to acquire per block.",
        rich_help_panel="Processing Options",
    ),
    max_blocks=typer.Option(
        help="Maximum number of compute node blocks to acquire.",
        rich_help_panel="Processing Options",
    ),
    staging_dir=typer.Option(
        help="Staging directory for processing.", rich_help_panel="Processing Options"
    ),
    clear_manifest=typer.Option(
        help="Clear existing manifest and start from scratch",
        rich_help_panel="Processing Options",
    ),
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
    run_batch_size=typer.Option(
        help="Number of datasets to process in a single flow batch.",
        rich_help_panel="Processing Options",
    ),
    dataset_limit=typer.Option(
        help="Limit number of datasets to process overall.",
        rich_help_panel="Processing Options",
    ),
    dataset_limit_to_first=typer.Option(
        help="Limit processing to first N datasets.",
        rich_help_panel="Processing Options",
    ),
    active_runs_limit=typer.Option(
        help="Maximum number of active runs to allow before starting a new run.",
        rich_help_panel="Processing Options",
    ),
    flow_debug=typer.Option(
        help="Enable flow debug output.", rich_help_panel="Processing Options"
    ),
)

CORR_ARGS = dict(
    qmap=typer.Option(
        "--qmap",
        "-q",
        help="Path to the qmap file",
        rich_help_panel="Corr Options",
    ),
    cycle=typer.Option(
        "--cycle",
        "-c",
        help="cycle for the dataset. Ex: 2025-1. Determines publish location.",
        rich_help_panel="Corr Options",
    ),
    type=typer.Option(
        "--type",
        "-t",
        help="Analysis type to be performed.",
        rich_help_panel="Corr Options",
    ),
    gpu_id=typer.Option(
        "--gpu-id",
        "-i",
        help="Choose which GPU to use. if the input is -1, then CPU is used",
        rich_help_panel="Corr Options",
    ),
    batch_size=typer.Option(
        help="Size of gpu corr processing batch",
        rich_help_panel="Corr Options",
    ),
    verbose=typer.Option(
        "--verbose",
        "-v",
        help="Verbose output",
        rich_help_panel="Corr Options",
    ),
    smooth=typer.Option(
        "--smooth",
        "-s",
        help="Smooth method to be used in Twotime correlation.",
        rich_help_panel="Corr Options",
    ),
    save_g2=typer.Option(
        "--save-g2",
        "-G",
        help="Save G2, IP, and IF to file.",
        rich_help_panel="Corr Options",
    ),
    avg_frame=typer.Option(
        "--avg-frame",
        "-a",
        help="Defines the number of frames to be averaged before the correlation.",
        rich_help_panel="Corr Options",
    ),
    begin_frame=typer.Option(
        "--begin-frame",
        "-b",
        help="Specifies which frame to begin with for the correlation. ",
        rich_help_panel="Corr Options",
    ),
    end_frame=typer.Option(
        "--end-frame",
        "-e",
        help="Specifies the last frame used for the correlation.",
        rich_help_panel="Corr Options",
    ),
    stride_frame=typer.Option(
        "--stride-frame",
        "-f",
        help="Defines the stride.",
        rich_help_panel="Corr Options",
    ),
    overwrite=typer.Option(
        "--overwrite",
        "-w",
        help="Overwrite the existing result file.",
        rich_help_panel="Corr Options",
    ),
    dq_selection=typer.Option(
        "--dq-selection",
        "-d",
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
    walltime: str = "1:00:00",
    nodes_per_block: int = 1,
    max_blocks: int = 5,
    flow_debug: bool = False,
    run_batch_size: int = 150,
    boost_corr_args: dict = None,
):
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
        run_batch_size (int): Number of datasets per flow batch.
        boost_corr_args (dict): Arguments for boost_corr tool. See CORR_ARGS for details.
    """

    if not source_files:
        return list()

    source_base = pathlib.Path(source_files[0]).parent.parent
    source_paths = [
        pathlib.Path(DEPLOYMENT.source_collection.to_globus(f)) for f in source_files
    ]
    staging_path = (
        pathlib.Path(DEPLOYMENT.get_input()["input"]["staging_dir"]) / staging_dir
    )

    # Chunk the files into batches, using relative paths from source base
    files = [f.relative_to(source_base) for f in source_paths]
    batches = [
        list(files[i : i + run_batch_size])
        for i in range(0, len(files), run_batch_size)
    ]

    finalized_batches = list()
    for idx, file_batch in enumerate(batches):

        transfer_items = list()
        xpcs_boost_corr_tasks = list()
        for item in file_batch:
            input_dataset = str(
                pathlib.Path(DEPLOYMENT.staging_collection.to_globus(staging_path))
                / item
                / "input"
            )
            transfer_items.append(
                {
                    "source_path": str(source_base / item),
                    "destination_path": input_dataset,
                    "recursive": True,
                }
            )

            task_kwargs = {
                "corr_input_file": "/eagle/APSDataProcessing/aps8idi/" + input_dataset
            }
            if flow_debug:
                task_kwargs["flow_debug"] = flow_debug
            task = {
                "function_id": get_function_id("xpcs_boost_corr_function_id"),
                "kwargs": task_kwargs,
            }
            xpcs_boost_corr_tasks.append(task)

        # Append qmap transfer item
        qmap = pathlib.Path(qmap)
        qmap_transfer_item = {
            "destination_path": str(
                pathlib.Path(DEPLOYMENT.staging_collection.to_globus(staging_path))
                / qmap.name
            ),
            "recursive": False,
            "source_path": str(DEPLOYMENT.source_collection.to_globus(str(qmap))),
        }
        transfer_items.append(qmap_transfer_item)

        # See if we can pack 100 datasets into a transfer
        flow_input = {
            "input": {
                "compute_queue": queue,
                "compute_endpoint": DEPLOYMENT.compute_endpoints["compute_endpoint"],
                "compute_walltime": walltime,
                "compute_nodes_per_block": nodes_per_block,
                "compute_max_blocks": max_blocks,
                "staging_base_path": str(staging_path),
                "staging_qmap": qmap_transfer_item["destination_path"],
                # "boost_corr": get_boost_corr_defaults(),
                "boost_corr": boost_corr_args,
                "qmap": "/eagle/APSDataProcessing/aps8idi"
                + qmap_transfer_item["destination_path"],
                "xpcs_boost_corr_tasks": xpcs_boost_corr_tasks,
                "source_transfer": {
                    "destination_endpoint_id": DEPLOYMENT.staging_collection.uuid,
                    "source_endpoint_id": DEPLOYMENT.source_collection.uuid,
                    "transfer_items": transfer_items,
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
        year, trimester = cycle.split("-")
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
    return [
        source_path / d["name"]
        for d in fetch_source_directory_metadata(path, collection_uuid, filter_type)
    ]


@app.command()
def list_cycles():
    print(
        f"Listing Cycles for endpoint {DEPLOYMENT.source_collection.uuid}, path {SOURCE_ENDPOINT_BASE_PATH}:"
    )
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
    if path:
        path = pathlib.Path(path)
    else:
        path = SOURCE_ENDPOINT_BASE_PATH / cycle / experiment / "data"
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

    if not experiment or not cycle:
        try:
            experiment, cycle = get_experiment_and_cycle_from_path(path)
        except ValueError as e:
            print(str(e))
            print(
                "Must provide experiment and cycle if they cannot be parsed from path!"
            )
            return

    path = path or SOURCE_ENDPOINT_BASE_PATH / cycle / experiment / "data"
    manifest_file = get_manifest_file_path(experiment, cycle)
    manifest = get_experiment_manifest(
        experiment_data_path=pathlib.Path(path),
        manifest_file=manifest_file,
    )

    parent_paths = [
        str(pathlib.Path(p["path"]).parent) for p in manifest.get("datasets", [])
    ]
    uniq_parent_paths = set(parent_paths)
    for p in uniq_parent_paths:
        print(f" - {p} -- {parent_paths.count(p)} datasets")


@app.command()
def update_manifest_status(
    experiment: Annotated[str, PROCESSING_ARGS["experiment"]],
    cycle: Annotated[str, CORR_ARGS["cycle"]],
):
    manifest_file = get_manifest_file_path(experiment, cycle)
    manifest = json.loads(pathlib.Path(manifest_file).read_text())
    while True:
        active = update_manifest(manifest=manifest, manifest_file=manifest_file)
        failed = [
            d for d in manifest.get("datasets", []) if d.get("dataset_status") == "FAILED"
        ]
        succeeded = [
            d
            for d in manifest.get("datasets", [])
            if d.get("dataset_status") == "SUCCEEDED"
        ]
        with_runs = len([d for d in manifest.get('datasets', []) if d.get("run_id")])
        total = len(manifest.get('datasets', []))
        print(
            f"Active runs: {active}, Failed datasets: {len(failed)}, Succeeded datasets: {len(succeeded)}, total: {with_runs}/{total}"
        )
        if active == 0:
            break


@app.command()
def run_experiment_subdirectory(
    path: Annotated[
        str,
        typer.Argument(
            help="Path to the experiment subdirectory to process. Example: /8IDI/2025-2/tempus202507-merge/data/converted/Cb0164_D100_a0060_f2000000"
        ),
    ],
    qmap: Annotated[str, CORR_ARGS["qmap"]],
    cycle: Annotated[str, CORR_ARGS["cycle"]] = None,
    experiment: Annotated[str, PROCESSING_ARGS["experiment"]] = None,
    deployment: Annotated[str, PROCESSING_ARGS["deployment"]] = "voyager-8idi-polaris",
    staging_dir: Annotated[str, PROCESSING_ARGS["staging_dir"]] = None,
    group: Annotated[
        str, PROCESSING_ARGS["group"]
    ] = "368beb47-c9c5-11e9-b455-0efb3ba9a670",
    queue: Annotated[
        PolarisQueues, PROCESSING_ARGS["queue"]
    ] = PolarisQueues.preemptable,
    walltime: Annotated[str, PROCESSING_ARGS["walltime"]] = "1:00:00",
    nodes_per_block: Annotated[int, PROCESSING_ARGS["nodes_per_block"]] = 1,
    max_blocks: Annotated[int, PROCESSING_ARGS["max_blocks"]] = 5,
    run_batch_size: Annotated[int, PROCESSING_ARGS["run_batch_size"]] = 200,
    flow_debug: Annotated[bool, PROCESSING_ARGS["flow_debug"]] = False,
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
    """
    Run boost corr processing on all datasets in the given experiment subdirectory.

    An example is this:

    python xpcs_batch_client.py run-experiment-subdirectory \
    --qmap /8IDI/2025-2/tempus202507-merge/data/timepix_Sq90_Dq9_log.hdf \
    --queue debug \
    /8IDI/2025-2/tempus202507-merge/data/converted/Cb0164_D100_a0060_f2000000

    Which will run the experiment subdirectory above on the debug queue. All datasets within the subdirectory
    will be batched and processed together.

    """
    if not experiment or not cycle:
        try:
            experiment, cycle = get_experiment_and_cycle_from_path(path)
        except ValueError as e:
            print(str(e))
            print(
                "Must provide experiment and cycle if they cannot be parsed from path!"
            )

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow_client = XPCSBoostBatch(flows_manager=flows_manager)

    source_files = fetch_source_directory(path=path, filter_type="dir")
    if dataset_limit > 0:
        source_files = source_files[:dataset_limit]

    if flow_debug:
        source_files = source_files[:1]
        queue = PolarisQueues.debug
        print("Flow debug enabled, limiting to 1 dataset on the debug queue.")

    staging_dir = (
        staging_dir
        or f"{queue.value}-{datetime.date.today().isoformat()}-{cycle}-{experiment}-{len(source_files)}"
    )

    batches = get_flow_batch_input(
        source_files=source_files,
        qmap=qmap,
        staging_dir=staging_dir,
        queue=queue.value,
        walltime=walltime,
        nodes_per_block=nodes_per_block,
        max_blocks=max_blocks,
        flow_debug=flow_debug,
        run_batch_size=run_batch_size,
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
        label = f"{batch['batch_size']}-{queue.value}-{idx + 1}-of-{len(batches)}-{pathlib.Path(path).name}"
        run = batch_flow_client.run_flow(
            flow_input=batch["flow_input"],
            label=label,
            tags=["aps", "xpcs", "batch-test", "test"],
        )
        batch["run_id"] = run["run_id"]
        print(
            f"Started run with {batch['batch_size']} datasets. https://app.globus.org/runs/{run['run_id']}/logs"
        )


def get_manifest_file_path(experiment: str, cycle: str):
    return pathlib.Path(f"manifest-{cycle}-{experiment}.json")


def get_experiment_manifest(
    experiment_data_path: pathlib.Path,
    manifest_file: pathlib.Path,
):
    if manifest_file.exists():
        print(f"Loading existing manifest from {manifest_file}")
        return json.loads(manifest_file.read_text())

    print("Building experiment manifest...")
    manifest = dict(datasets=list())
    for exp_subdir in fetch_source_directory(
        path=experiment_data_path, filter_type="dir"
    ):
        datasets = fetch_source_directory(
            path=experiment_data_path / exp_subdir, filter_type="dir"
        )
        print(f"Adding {len(datasets)} datasets from subdirectory {exp_subdir}")
        for dataset in datasets:
            manifest["datasets"].append(
                {"path": str(experiment_data_path / exp_subdir / dataset)}
            )

    manifest_file.write_text(json.dumps(manifest, indent=2))
    print(f"Wrote manifest to {manifest_file}")
    return manifest


def get_run_map(session, client):
    flows_client = globus_sdk.FlowsClient(app=globus_app)
    query_params = dict(orderby="start_time DESC", per_page=50)
    runs = {}
    # Fetch recent runs
    for idx, page in enumerate(
        flows_client.paginated.list_runs(query_params=query_params)
    ):
        if idx >= 4:
            break
        # print(f"Fetching page {idx} of run list...")
        for run in page["runs"]:
            runs[run["run_id"]] = run
    return runs


def get_dataset_status(run_data, dataset):
    if run_data["status"] == "SUCCEEDED":
        for task in run_data["details"]["output"]["XpcsBoostCorr"]["details"]["results"]:
            if dataset["path"].endswith(task["output"]["id"]):
                return task["output"]["result"]
    raise Exception("Failed to find dataset status in run output!")


def update_manifest(manifest: dict, manifest_file: pathlib.Path, updates: dict = None):
    """
    Update a manifest by checking each dataset with a run id, and checking inside
    each run id for the success or failure of a dataset.
    
    """
    run_map = get_run_map(globus_app, globus_sdk.FlowsClient)
    updates = updates or dict()
    active_runs = set()
    for idx, dataset in enumerate(manifest["datasets"]):
        # Update any datasets with a run id
        new_run = updates.get(dataset["path"])
        if new_run:
            dataset["run_id"] = new_run
            dataset["run_status"] = "ACTIVE"

        run_id = dataset.get("run_id")
        if not run_id:
            # Disregard anything without a run_id
            continue
        elif run_id and dataset.get("dataset_status") in ["SUCCEEDED", "FAILED"]:
            # Disregard old completed runs
            continue

        # Run id isn't in the recent map. Fetch it manually
        if run_id and run_id not in run_map:
            print("Need to fetch run id manually")
            flows_client = globus_sdk.FlowsClient(app=globus_app)
            run_map[run_id] = flows_client.get_run(run_id)

        # Update the dataset in the manifest
        dataset["run_status"] = run_map[run_id]["status"]
        if dataset["run_status"] == "SUCCEEDED":
            dataset["dataset_status"] = get_dataset_status(run_map[run_id], dataset)
        else:
                dataset["dataset_status"] = dataset["run_status"]
        (
            active_runs.add(dataset["run_id"])
            if dataset["run_status"] == "ACTIVE"
            else None
        )
    manifest_file.write_text(json.dumps(manifest, indent=2))
    return len(active_runs)


@app.command()
def run_experiment(
    path: str,
    qmap: Annotated[str, CORR_ARGS["qmap"]],
    cycle: Annotated[str, CORR_ARGS["cycle"]] = None,
    experiment: Annotated[str, PROCESSING_ARGS["experiment"]] = None,
    deployment: Annotated[str, PROCESSING_ARGS["deployment"]] = "voyager-8idi-polaris",
    staging_dir: Annotated[str, PROCESSING_ARGS["staging_dir"]] = None,
    group: Annotated[
        str, PROCESSING_ARGS["group"]
    ] = "368beb47-c9c5-11e9-b455-0efb3ba9a670",
    queue: Annotated[
        PolarisQueues, PROCESSING_ARGS["queue"]
    ] = PolarisQueues.preemptable,
    walltime: Annotated[str, PROCESSING_ARGS["walltime"]] = "1:00:00",
    nodes_per_block: Annotated[int, PROCESSING_ARGS["nodes_per_block"]] = 1,
    max_blocks: Annotated[int, PROCESSING_ARGS["max_blocks"]] = 5,
    run_batch_size: Annotated[int, PROCESSING_ARGS["run_batch_size"]] = 200,
    flow_debug: Annotated[bool, PROCESSING_ARGS["flow_debug"]] = False,
    active_runs_limit: Annotated[int, PROCESSING_ARGS["active_runs_limit"]] = 20,
    dataset_limit: Annotated[int, PROCESSING_ARGS["dataset_limit"]] = 0,
    dataset_limit_to_first: Annotated[
        int, PROCESSING_ARGS["dataset_limit_to_first"]
    ] = 0,
    clear_manifest: Annotated[bool, PROCESSING_ARGS["clear_manifest"]] = False,
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
            print(
                "Must provide experiment and cycle if they cannot be parsed from path!"
            )
            return
    manifest_file = get_manifest_file_path(experiment, cycle)
    manifest = get_experiment_manifest(
        experiment_data_path=pathlib.Path(path),
        manifest_file=manifest_file,
    )

    if flow_debug:
        dataset_limit = 1
        print("Flow debug enabled, limiting to 1 dataset.")

    if dataset_limit_to_first > 0:
        manifest["datasets"] = manifest["datasets"][:dataset_limit_to_first]
        print(f"Limiting to first {dataset_limit_to_first} datasets.")

    if clear_manifest:
        print("Clearing existing manifest run IDs...")
        for d in manifest["datasets"]:
            d.pop("run_id", None)
            d.pop("run_status", None)
            if d.get("dataset_status"):
                d.pop("dataset_status")
        manifest_file.write_text(json.dumps(manifest, indent=2))

    datasets = [d for d in manifest["datasets"] if not d.get("run_id")]
    print(
        f"Found {len(datasets)} unprocessed datasets in experiment {experiment} cycle {cycle}."
    )
    if dataset_limit > 0:
        datasets = datasets[:dataset_limit]
        print(f"Limiting to {dataset_limit} datasets.")

    work_queue = SimpleQueue()

    staging_dir = staging_dir or f"{queue.value}-{cycle}-{experiment}-{len(datasets)}"

    batches = get_flow_batch_input(
        source_files=[d["path"] for d in datasets],
        qmap=qmap,
        staging_dir=staging_dir,
        queue=queue.value,
        walltime=walltime,
        nodes_per_block=nodes_per_block,
        max_blocks=max_blocks,
        flow_debug=flow_debug,
        run_batch_size=run_batch_size,
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
        work_item = {
            "batch_index": idx,
            "flow_data": {
                "flow_input": batch["flow_input"],
                "label": f"exp-test-{run_batch_size}-{queue.value}-{idx + 1}-of-{len(batches)}",
                "tags": ["aps", "xpcs", "batch-test", "test"],
            },
        }
        work_queue.put(work_item)

    flows_manager = FlowsManager(run_kwargs=run_kwargs)
    batch_flow_client = XPCSBoostBatch(flows_manager=flows_manager)
    active_runs = update_manifest(manifest, manifest_file)
    initial_queue_size = work_queue.qsize()
    print(
        f"Starting processing of {initial_queue_size} batches with max concurrency {active_runs_limit}..."
    )
    while not work_queue.empty():
        num_runs_to_start = active_runs_limit - active_runs
        dataset_updates = {}
        print(f"Starting {num_runs_to_start}")
        for num in range(num_runs_to_start):
            if work_queue.empty():
                break

            work_item = work_queue.get()
            run = batch_flow_client.run_flow(**work_item["flow_data"])
            print(
                f"Started {work_item['flow_data']['label']} with run ID {run['run_id']}"
            )
            source_paths = [
                ti["source_path"]
                for ti in work_item["flow_data"]["flow_input"]["input"][
                    "source_transfer"
                ]["transfer_items"]
            ]
            d_update = {p: run["run_id"] for p in source_paths}
            dataset_updates.update(d_update)

        print("Updating manifest")
        active_runs = update_manifest(manifest, manifest_file, dataset_updates)
        print(
            f"Active runs: {active_runs}, Batches remaining: {work_queue.qsize()}/{initial_queue_size}"
        )


if __name__ == "__main__":
    try:
        app()
    except globus_sdk.TransferAPIError as e:
        print(f"Globus Transfer API Error: {e.message}")
        sys.exit(1)
