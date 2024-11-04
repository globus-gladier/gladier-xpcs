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
from gladier_xpcs.flows import XPCSBoost
from gladier import FlowsManager


FILTER_RANGE_MIN = datetime.datetime.now(tz=zoneinfo.ZoneInfo('UTC')) - datetime.timedelta(days=3)
FILTER_RANGE_MAX = datetime.datetime.now(tz=zoneinfo.ZoneInfo('UTC'))
FLOW_CLASS = XPCSBoost
FLOW_ID = '56c933db-16c3-4416-b8df-6fa31379a602'
RUNS_CACHE = f'/tmp/{FLOW_CLASS.__name__}RunsCache.json'
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


def load_cache():
    try:
        with open(RUNS_CACHE) as f:
            return json.load(f)
    except FileNotFoundError:
        return dict()


def get_run_cache_age() -> int:
    return int(time.time() - load_cache().get('timestamp', -1))


def get_run_cache(cache_ttl=CACHE_TTL) -> list:
    return load_cache()['runs'] if is_cached(cache_ttl=cache_ttl) else None


def save_run_cache(runs):
    with open(RUNS_CACHE, 'w') as f:
        json.dump({'runs': runs, 'timestamp': time.time()}, f)


def get_runs(flow_id, cache_ttl=CACHE_TTL):
    if USE_CACHE and is_cached(cache_ttl):
        return get_run_cache(cache_ttl)
    client = get_client()
    client.login()

    fc = client.flows_manager.flows_client
    rng = f'{FILTER_RANGE_MIN.isoformat(timespec="seconds")},{FILTER_RANGE_MAX.isoformat(timespec="seconds")}'
    resp = fc.list_runs(filter_flow_id=FLOW_ID, query_params=dict(filter_start_time=rng))
    runs = resp['runs']
    pages = 0
    while resp['has_next_page']:
        pages += 1
        resp = fc.list_runs(filter_flow_id=FLOW_ID, marker=resp['marker'])
        runs += resp['runs']

        # For admins, desperate for continuous feedback
        print('.', end='')
        sys.stdout.flush()

    print()
    save_run_cache(runs)
    return runs


def get_run_input(run_id, flow_id, scope=None):
    resp = get_flows_client().get_run_logs(run_id)
    input_payload = resp['entries'][0]['details']['input']

    # These should be removed, but needed for old runs pre Gladier v0.9
    input_payload['input']['login_node_compute'] = input_payload['input'].get('login_node_compute', input_payload['input']['compute_endpoint_non_compute'])
    input_payload['input']['compute_endpoint'] = input_payload['input'].get('compute_endpoint', input_payload['input']['compute_endpoint_compute'])

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


def get_runs_since_label(runs, label):
    """
    Find the earliest occurance in which the run with a given label has failed, and
    return all runs since that point. If multiple runs include the label, this function
    will return the first occurance.
    """
    bounding_run = -1
    for run in runs:
        if run['label'] == label:
            bounding_run = runs.index(run)
    if bounding_run < 0:
        raise ValueError(f'Failed to find {label} in {len(runs)} total runs.')
    return runs[bounding_run:]


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
@click.option('--status', default='FAILED', help='Flow id to use')
@click.option('--preview', is_flag=True, default=False, help='Flow id to use')
@click.option('--since', help='Re-run all failed jobs since the label of this failed job')
@click.option('--workers', help='Number of parallel processing jobs', default=30)
def retry_runs(flow, local_fx, status, preview, since, workers):
    runs = [run for run in get_runs(flow) if run['status'] == status]
    runs = sort_runs(runs)
    if since:
        try:
            runs = get_runs_since_label(runs, label=since)
        except ValueError as ve:
            click.secho(str(ve), fg='red')
            return
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
