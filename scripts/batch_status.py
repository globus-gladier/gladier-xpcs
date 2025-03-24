import pprint
import os
import sys
import datetime
import zoneinfo
import time
import json
import queue
import threading
import click
import globus_sdk
import pathlib
import asyncio
import collections
from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier import FlowsManager


FILTER_RANGE_MIN = datetime.datetime.now(tz=zoneinfo.ZoneInfo('UTC')) - datetime.timedelta(days=3)
FILTER_RANGE_MAX = datetime.datetime.now(tz=zoneinfo.ZoneInfo('UTC'))
FILTER_RANGE_MIN = datetime.datetime.now(
    tz=zoneinfo.ZoneInfo("UTC")
) - datetime.timedelta(days=20)
FILTER_RANGE_MAX = datetime.datetime.now(tz=zoneinfo.ZoneInfo("UTC"))
FLOW_CLASS = XPCSBoost
FLOW_ID = '193373a8-8040-4267-aea6-a41f171e7f96'
RUNS_CACHE = f'/tmp/{FLOW_CLASS.__name__}RunsCache.json'
FLOW_ID = "56c933db-16c3-4416-b8df-6fa31379a602"
RUNS_CACHE = f"/tmp/{FLOW_CLASS.__name__}RunsCache.json"
RUN_LOGS_CACHE = f"/tmp/{FLOW_CLASS.__name__}RunLogsCache.json"
# Keep cache for a week
CACHE_TTL = 60 * 60 * 24 * 7
USE_CACHE = False
RUN_QUEUE = queue.Queue()

__CACHED_CLIENT = None


RUN_FIELDS = [
    'status',
    'run_id',
    'label',
    'start_time',
    # 'action_id',
    # 'completion_time',
    # 'created_by',
    # 'details',
    # 'display_status',
    # 'flow_id',
    # 'flow_last_updated',
    # 'flow_title',
    # 'manage_by',
    # 'monitor_by',
    # 'run_managers',
    # 'run_monitors',
    # 'run_owner',
    # 'user_role'
]

def get_client():
    global __CACHED_CLIENT
    if __CACHED_CLIENT:
        return __CACHED_CLIENT
    if os.getenv('GLADIER_CLIENT_ID') and os.getenv('GLADIER_CLIENT_SECRET'):
        __CACHED_CLIENT = FLOW_CLASS(flows_manager=FlowsManager(flow_id=FLOW_ID))
        __CACHED_CLIENT.login()
        return __CACHED_CLIENT
    raise ValueError('Warning, only service clients are allowed. Define "GLADIER_CLIENT_ID" and "GLADIER_CLIENT_SECRET"')

def get_flows_client():
    return get_client().flows_manager.flows_client


def is_cached(cache_ttl=CACHE_TTL) -> bool:
    return get_run_cache_age() < cache_ttl


def load_cache(filename=RUNS_CACHE) -> dict:
    try:
        with open(filename) as f:
            return json.load(f)
    except (FileNotFoundError, json.decoder.JSONDecodeError):
        return dict()


def get_run_cache_age() -> int:
    return int(time.time() - load_cache().get('timestamp', -1))


def get_run_cache(cache_ttl=CACHE_TTL) -> list:
    return load_cache()['runs'] if is_cached(cache_ttl=cache_ttl) else None


def save_run_cache(runs):
    with open(RUNS_CACHE, 'w') as f:
        json.dump({'runs': runs, 'timestamp': time.time()}, f)


def get_run_logs_cache(cache_ttl=CACHE_TTL) -> list:
    return (
        load_cache(RUN_LOGS_CACHE).get("run_logs")
        if is_cached(cache_ttl=cache_ttl)
        else None
    )


def save_run_logs_cache(run_logs):
    with open(RUN_LOGS_CACHE, "w") as f:
        json.dump({"run_logs": run_logs, "timestamp": time.time()}, f)


def get_query_params(since_days=0):
    query_params = {"orderby": ("start_time DESC",)}
    if since_days > 0:
        min_filter = datetime.datetime.now(
            tz=zoneinfo.ZoneInfo("UTC")
        ) - datetime.timedelta(days=since_days)
        max_filter = datetime.datetime.now(tz=zoneinfo.ZoneInfo("UTC"))
        rng = f'{min_filter.isoformat(timespec="seconds")},{max_filter.isoformat(timespec="seconds")}'
        query_params["filter_start_time"] = rng
    return query_params


def get_runs(flow_id, cache_ttl=CACHE_TTL, since_days=0):
    if USE_CACHE and is_cached(cache_ttl):
        return get_run_cache(cache_ttl)
    client = get_client()
    client.login()

    fc = client.flows_manager.flows_client
    run_list = []
    print("Fetching runs with parameters: ", get_query_params(since_days=since_days))
    for resp in fc.paginated.list_runs(
        query_params=get_query_params(since_days=since_days)
    ):
        run_list += resp.data["runs"]

        # For admins, desperate for continuous feedback
        print('.', end='')
        sys.stdout.flush()

    print()
    save_run_cache(run_list)
    return run_list


def get_run_input(run_id, flow_id, scope=None):
    resp = get_flows_client().get_run_logs(run_id)
    input_payload = resp["entries"][0]["details"]["input"]
    return input_payload


def retry_single(run_id, flow_id, scope=None, use_local=False):
    run_input = get_run_input(run_id, flow_id, scope)
    label = pathlib.Path(run_input['input']['hdf_file']).name[:62]
    # Check for all values that resemble globus compute functions and remove them.
    # Gladier will replace them with local functions
    if use_local:
        for k in list(run_input['input'].keys()):
            if k.endswith('_function_id'):
                run_input['input'].pop(k)
    return get_client().run_flow(flow_input=run_input, label=label)


def run_worker():
    flow_client_instance = get_client()
    while True:
        run, flow_id, kwargs = RUN_QUEUE.get()
        try:
            resp = retry_single(run['run_id'], flow_id, **kwargs)
            if resp is None:
                print(f'Failed retry: {run["label"]}: {run["run_id"]}')
            print(f'Retried {resp["label"]} (https://app.globus.org/runs/{resp["run_id"]})')
            status = None
            while status not in ['SUCCEEDED', 'FAILED']:
                status = flow_client_instance.get_status(resp['run_id']).get('status')
                time.sleep(30)
            if status == 'FAILED':
                print(f'Run FAILED: {resp["label"]} ({run["run_id"]}):  https://app.globus.org/runs/{resp["run_id"]}')

        except globus_sdk.exc.GlobusAPIError as gapie:
            print(f'Failed retry: {resp["label"]} ({run["run_id"]}), message: {gapie.message}')
        RUN_QUEUE.task_done()


def make_csv(runs, sort_field='start_time'):
    # Make the CSV
    summary = [[run[f] for f in RUN_FIELDS] for run in sort_runs(runs, sort_field=sort_field)]
    formatted = [','.join(RUN_FIELDS)] + [','.join(str(f) for f in run) for run in summary]
    return '\n'.join(formatted)


def sort_runs(runs, sort_field='start_time'):
    if sort_field not in RUN_FIELDS:
        raise ValueError(f'"{sort_field}" is not in RUN_FIELDS. Please add it.')
    return sorted(runs, key=lambda x: x[sort_field])


async def _update_single_run_log(
    worker_name: str,
    flows_client: globus_sdk.FlowsClient,
    queue,
    run_logs: dict,
):
    print(f"Worker {worker_name} started.")
    while not queue.empty():
        run_id = await queue.get()
        run_log = await asyncio.to_thread(flows_client.get_run_logs, run_id, limit=1)
        assert run_log.data["entries"][0]["code"] == "FlowStarted"
        run_logs[run_id] = run_log.data["entries"][0]["details"]["input"]
        queue.task_done()


async def _update_run_logs_loop(runs, run_logs):
    # Prep the queue
    fetch_queue = asyncio.Queue()
    for run in runs:
        if run["run_id"] not in run_logs:
            fetch_queue.put_nowait(run["run_id"])

    if fetch_queue.empty():
        return
    else:
        print(f"Fetching {fetch_queue.qsize()} logs")

    initial_size = fetch_queue.qsize()
    flows_client = get_client().flows_manager.flows_client

    tasks = []
    for i in range(3):
        task = asyncio.create_task(
            _update_single_run_log(f"worker-{i}", flows_client, fetch_queue, run_logs)
        )
        tasks.append(task)

    while not fetch_queue.empty():
        print(f"Working on queue ({initial_size - fetch_queue.qsize()}/{initial_size})")
        await asyncio.sleep(1)
    await fetch_queue.join()
    await asyncio.gather(*tasks, return_exceptions=True)


def update_run_logs(runs):

    run_logs = get_run_logs_cache() or dict()
    exit_now = False

    try:
        asyncio.run(_update_run_logs_loop(runs, run_logs))
    except KeyboardInterrupt:
        print("Interrupt Received! Saving and exiting...")
        exit_now = True
    finally:
        save_run_logs_cache(run_logs)
        if exit_now:
            sys.exit(1)
    return run_logs


def filter_unsuccessful_failure_runs(runs, run_logs):
    """
    Filter any runs that have previously failed, and have not been retried successfully.

    Uniqueness is based on the output.input.hdf_file field.
    """
    runs_by_hdf = collections.defaultdict(list)
    for idx, run in enumerate(runs):
        try:
            hdf_file = run_logs[run["run_id"]]["input"]["hdf_file"]
        except KeyError as ke:
            print("Failed at item", idx)
            raise ke
        runs_by_hdf[hdf_file].append(run)

    # Iterate thorugh runs, and filter out any that have a successful run
    filtered_runs = []
    for run in runs:
        hdf_file = run_logs[run["run_id"]]["input"]["hdf_file"]
        if any(r["status"] == "SUCCEEDED" for r in runs_by_hdf[hdf_file]):
            print(
                f"Skipping {run['label']} as it has a successful run at date {run['start_time']}"
            )
            continue
        # If a run has failures, and no successful runs, only choose the most recent failure.
        if datetime.datetime.fromisoformat(run["start_time"]) == max(
            datetime.datetime.fromisoformat(r["start_time"])
            for r in runs_by_hdf[hdf_file]
        ):
            print(f"Adding last failed run {run['label']} at date {run['start_time']}")
            filtered_runs.append(run)
        else:
            print(
                f"Skipping {run['label']} as it has a more recent failure at date {run['start_time']}"
            )
            print(
                f"\t{run['start_time']} vs {max(datetime.datetime.fromisoformat(r['start_time']) for r in runs_by_hdf[hdf_file])}"
            )

    print(f"Filtered {len(runs)} runs down to {len(filtered_runs)} runs.")
    return filtered_runs


def get_runs_since_label(runs, label):
    """
    Find the earliest occurance in which the run with a given label has failed, and
    return all runs since that point. If multiple runs include the label, this function
    will return the first occurance.
    """
    bounding_run = -1
    total = len(runs)
    for run in runs:
        if run['label'] == label:
            bounding_run = runs.index(run)
    if bounding_run < 0:
        raise ValueError(f"Failed to find {label} in {len(runs)} total runs.")
    runs = runs[bounding_run:]
    print(
        f"Found Bounding run at index {bounding_run}, with num runs: {len(runs)}, discarding {bounding_run}/{total} runs."
    )
    return runs


@click.group()
@click.option('--cached', is_flag=True, default=False, help='Re-use the list of runs the last time this script was used.')
def batch_status(cached):
    global USE_CACHE
    if cached:
        click.secho(f'Last cache was {get_run_cache_age()} seconds ago at {RUNS_CACHE} (Max {CACHE_TTL}). Cache is fresh enough? {is_cached()}', err=True)
        USE_CACHE=True


@batch_status.command()
@click.option('--flow', help='Flow id to use')
def csv(flow):
    click.secho(make_csv(sort_runs(get_runs(flow))))


@batch_status.command()
@click.option('--flow', help='Flow id to use')
def summary(flow):
    statuses = [r['status'] for r in get_runs(flow)]
    output = sorted([f'{status_type}: {statuses.count(status_type)}'
                    for status_type in set(statuses)])
    output.append(f'Total Runs: {len(statuses)}')
    click.secho(', '.join(output))


@batch_status.command()
@click.option('--run', help='Run to retry', required=True)
@click.option('--flow', default=None, help='Flow id to use')
@click.option('--local-fx', default=False, is_flag=True,
help='Use local globus compute functions instead of the functions from the last run.')
def retry_run(run, flow, local_fx):
    resp = retry_single(run, flow, use_local=local_fx)
    click.secho(f'Retried {resp["label"]} (https://app.globus.org/runs/{resp["run_id"]})')


@batch_status.command()
@click.option('--run', help='Run to retry', required=True)
@click.option('--flow', default=None, help='Flow id to use')
def dump_run_input(run, flow):
    pprint.pprint(get_run_input(run, flow)['input'])


@batch_status.command()
@click.option('--flow', help='Flow id to use')
@click.option('--local-fx', default=False, is_flag=True,
    help='Use local globus compute functions instead of the functions from the last run.')
@click.option('--status', default=None, help='Flow id to use')
@click.option('--preview', is_flag=True, default=False, help='Flow id to use')
@click.option('--since', help='Re-run all failed jobs since the label of this failed job')
@click.option("--since-days", default=14, help="Re-run all failed jobs since the label of this failed job")
@click.option("--filter-unsuccessful-failures", is_flag=True, default=True, help="Filter out runs that have never succeeded.")
@click.option('--workers', help='Number of parallel processing jobs', default=30)
def retry_runs(flow, local_fx, status, preview, since, since_days, filter_unsuccessful_failures, workers):
    runs = [run for run in get_runs(flow, since_days=since_days)]
    runs = sort_runs(runs)
    if filter_unsuccessful_failures:
        run_logs = update_run_logs(runs)
        runs = filter_unsuccessful_failure_runs(runs, run_logs)
    if since:
        try:
            runs = get_runs_since_label(runs, label=since)
        except ValueError as ve:
            click.secho(str(ve), fg='red')
            return
    if status:
        runs = [run for run in runs if run['status'] == status]
    if preview == True:
        click.echo(make_csv(runs))
        click.echo(f'{len(runs)} above will be restarted.')
        click.confirm('re-run the above flows?', abort=True)
    fc = get_client().flows_manager.flows_client
    # Build up the queue
    for run in runs:
        args = (run, flow, dict(scope=get_client().flows_manager.flow_scope, use_local=local_fx))
        RUN_QUEUE.put(args)

    # Start threads to consume the queue
    for _ in range(workers):
        threading.Thread(target=run_worker, daemon=True).start()
        # Allow each thread a little time to start its own flow.
        # This will ensure we stagger runs a bit, and don't start all transfer tasks at the same time.
        time.sleep(1)

    try:
        # Monitor queue size and notify the user of progress
        while RUN_QUEUE.qsize() > 0:
            print(f'Remaining in queue: {RUN_QUEUE.qsize()}/{len(runs)}')
            time.sleep(30)
        click.secho('Waiting on final runs to complete...')
        RUN_QUEUE.join()
        click.secho('Done', fg='green')
    except KeyboardInterrupt:
        click.secho(f'Exiting due to user Interrupt. Queue was {RUN_QUEUE.qsize()}/{len(runs)}',
            fg='red')

if __name__ == '__main__':
    batch_status()
