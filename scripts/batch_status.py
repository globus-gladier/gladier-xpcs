import click
import pathlib
from gladier_xpcs.flow_online import XPCSOnlineFlow
from globus_automate_client.client_helpers import create_flows_client


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


def get_runs(flow_id=None):
    if flow_id is None:
        flow_id = XPCSOnlineFlow().get_flow_id()
    fc = create_flows_client()

    resp = fc.list_flow_runs(flow_id)
    runs = resp['actions']
    while resp['has_next_page']:
        resp = fc.list_flow_runs(flow_id, marker=resp['marker'])
        runs += resp['actions']
    return runs


def retry_single(run_id, flow_id=None):
    if flow_id is None:
        flow_id = XPCSOnlineFlow().get_flow_id()
    fc = create_flows_client()

    resp = fc.flow_action_log(flow_id, fc.scope_for_flow(flow_id), run_id)
    run_input = resp['entries'][0]['details']['input']
    label = pathlib.Path(run_input['input']['hdf_file']).name[:62]
    return XPCSOnlineFlow().run_flow(flow_input=run_input, label=label)


@click.group()
def batch_status():
    pass


@batch_status.command()
@click.option('--flow', default=None, help='Flow id to use')
def csv(flow):
    runs = get_runs(flow)
    click.secho(','.join(RUN_FIELDS))
    formatted = []
    for run in runs:
        formatted.append(','.join([f'{run[k]}' for k in RUN_FIELDS]))
    click.secho('\n'.join(formatted))


@batch_status.command()
@click.option('--flow', default=None, help='Flow id to use')
def summary(flow):
    statuses = [r['status'] for r in get_runs(flow)]
    output = sorted([f'{status_type}: {statuses.count(status_type)}'
                    for status_type in set(statuses)])
    output.append(f'Total Runs: {len(statuses)}')
    click.secho(', '.join(output))


@batch_status.command()
@click.option('--run', help='Run to retry', required=True)
@click.option('--flow', default=None, help='Flow id to use')
def retry_run(run, flow):
    resp = retry_single(run, flow)
    click.secho(f'Retried {resp["label"]} (https://app.globus.org/runs/{resp["run_id"]})')


if __name__ == '__main__':
    batch_status()

