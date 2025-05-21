#!/dmOpt/workflows/miniconda3/envs/gladier-0.9.4/bin/python

## /dmOpt/workflows/miniconda3/envs/gladier-0.9.4/bin/python /home/dm/workflows/xpcs8/gladier-xpcs/scripts/xpcs_online_boost_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

import argparse
import os
import sys
import pathlib
import time
import traceback

from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier_xpcs.deployments import BaseDeployment, deployment_map
from gladier_xpcs import log  # noqa Add INFO logging

from globus_sdk import ConfidentialAppAuthClient, AccessTokenAuthorizer, FlowsClient
from globus_sdk.exc.convert import GlobusConnectionError
from gladier.managers.login_manager import CallbackLoginManager

from typing import List, Mapping, Union
import traceback
from fair_research_login import JSONTokenStorage


# Get client id/secret
CLIENT_ID = os.getenv("GLADIER_CLIENT_ID")
CLIENT_SECRET = os.getenv("GLADIER_CLIENT_SECRET")
DEVELOPER_GROUP = 'urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670'

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment', help='Name of the DM experiment', default='comm202410')
    parser.add_argument('--hdf', help='Deprecated. Unused.', default=None)
    parser.add_argument('-r', '--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/G001_436_PorousGlass-08000/G001_436_PorousGlass-08000.h5')
    parser.add_argument('-q', '--qmap', help='Path to the qmap file',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/eiger4m_qmap_1018_hongrui_d36.h5')
    parser.add_argument('-c', '--cycle', help="cycle for the dataset. Ex: 2025-1. Determines publish location.", default=None)
    parser.add_argument('-t', '--type', default='Multitau', help='Analysis type to be performed.', choices=['Multitau', 'Both', 'Twotime'])
    parser.add_argument('-i', '--gpu-id', type=int, default=0, help='Choose which GPU to use. if the input is -1, then CPU is used')
    # Group MUST not be None in order for PublishTransferSetPermission to succeed. Group MAY
    # be specified even if the flow owner does not have a role to set ACLs, in which case PublishTransferSetPermission will be skipped.
    parser.add_argument('--group', help='Visibility in Search', default='368beb47-c9c5-11e9-b455-0efb3ba9a670')
    parser.add_argument('--deployment', default='voyager-8idi-polaris', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
    parser.add_argument('--batch_size', default='256', help=f'Size of gpu corr processing batch')
    parser.add_argument('-v', '--verbose', default=True, action='store_true', help=f'Verbose output')
    parser.add_argument('--skip-transfer-back', action='store_true', default=False, help="Skip transfer of processed data to source collection. "
                        "Should not be skipped in normal operation. Use this option only for testing or reprocessing old data.")
    parser.add_argument('-s', '--smooth', default='sqmap', help=f'Smooth method to be used in Twotime correlation.')
    parser.add_argument('-G', '--save-g2', default=False, action='store_true', help=f'Save G2, IP, and IF to file.')
    parser.add_argument('-a', '--avg-frame', default=1, type=int, help=f'Defines the number of frames to be averaged before the correlation.')
    parser.add_argument('-b', '--begin-frame', default=0, type=int, help=f'Specifies which frame to begin with for the correlation. ')
    parser.add_argument('-e', '--end-frame', default=-1, type=int, help=f'Specifies the last frame used for the correlation.')
    parser.add_argument('-f', '--stride-frame', default=1, type=int, help=f'Defines the stride.')
    parser.add_argument('-w', '--overwrite', default=True, action='store_true', help=f'Overwrite the existing result file.')
    parser.add_argument('-d', '--dq-selection', default='all', help=f'A string that selects the dq list, eg. \'1, 2, 5-7\' selects [1,2,5,6,7]')
    parser.add_argument('-o', '--output', help=f'This is the "transfer back" output directory on source, where the results corr file will be transferred.')

    return parser.parse_args()

def get_deployment(args_deployment: str, args_raw: str = ""):
    """
    Fetch the deployment based on the string given. Also checks service account related variables are set.
    :param args_deployment: A string denoting the name of the deployment in deployments.py
    :param args_raw: An optional "input" string to help detect the deployment, if on voyager
    """
    deployment = deployment_map.get(args_deployment)
    if "/gdata/dm/XPCS8" in args_raw:
        deployment = deployment_map.get('voyager-xpcs8-polaris')
    if not deployment:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')
    elif deployment.service_account and not (os.getenv('GLADIER_CLIENT_ID') and os.getenv('GLADIER_CLIENT_SECRET')):
        raise ValueError(f'Deployment requires setting GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET')
    return deployment

def determine_cycle(raw: str):
    """
    Attempt to determine the cycle from a given dataset directory. Typically it looks like this:
    
    /gdata/dm/8IDI/2025-1/experiment202503/data/foo-bar-baz/foo-bar-baz.bin.000

    Where the cycle would look like:

    2025-1

    :param raw: A raw file input. 
    :raises ValueError: If it fails to parse the cycle.
    """
    try:
        cycle = pathlib.Path(raw).relative_to("/gdata/dm/8IDI/").parts[0]
        year, trimester = cycle.split('-')
        if int(year) in range(2015, 2050) and int(trimester) in range(1, 4):
            return cycle
    except Exception as e:
        print(e)
    raise ValueError("Failed to automatically parse the cycle from the given input. Please specify it manually with '-c 2025-1'")


def get_filepaths(raw: str, qmap: str, output: str, experiment: str, deployment: BaseDeployment, cycle: str = None):
    '''
    Generate all paths to be used on the staging endpoint deployment as well as the publishing endpoint. Paths
    are generated using source filenames, mainly ``raw`` and ``qmap``. The file structure on staging is generated
    to contain 3 folders: 

    input/ -- contains all input files including the hdf metadata file needed by boost_corr
    output/ -- contains the output of running boost_corr and the plotting tool
    qmap/ -- Contains the qmap file

    The inputs typically look like this:
      /2025-1/milliron202503/data/04fsaxs13075_DMF-ethylene-glycol-50mM-salt-blank_a0208_f100000_r00005

    The outputs typically look like this: 

      /xpcs_staging/experiment/dataset_name/output/qmap_file_name/dataset_name/
        resources/
        dataset_name_results.hdf
        boost_corr.log

    Note: dataset_name is repeated twice. Typically, thousands of datasets reside in /xpcs_staging/experiment_name/.
    ``qmap_file_name/dataset_name/`` differentiates datasets run by different qmap files. This also ingests them into
    a different location on publishing, if multiple qmap files are used for multiple reprocessing times.


    :param raw: The source filename for the 'raw' file passed to boost_corr for processing. This is used to determine
                the processing directory structure on staging, including the dataset name and the _results.hdf file.
    :param qmap: The location of the qmap file.
    :param output: The location of the output file being sent back to voyager
    :param experiment: describes the experiment being run. Typically the name of the subdirectory under cycle Ex: milliron202503. Can
                       be customized for testing purposes.
    :param deployment: A BaseDeployment class which contains information about where to transfer the data and what compute to use.
    :param cycle: A string like 2025-1. Used for determining where to publish datasets. Can usually be automatically determined
                  from source files, but may need to be provided if the source file does not contain a cycle.
    :returns: A dict of info which can be passed to `get_flow_input` for starting the flow.
    '''
    depl_input = deployment.get_input()
    dataset_name = os.path.basename(raw).split('.')[0]  #remove file extension
    qmap_name = os.path.splitext(os.path.basename(qmap))[0] # The name of the qmap file without an extension
    dataset_staging_dir = os.path.join(depl_input['input']['staging_dir'], experiment, dataset_name)

    input_source = os.path.dirname(raw)
    input_staging = os.path.join(dataset_staging_dir, 'input')
    raw_dest = os.path.join(input_staging, os.path.basename(raw))
    qmap_dest = os.path.join(dataset_staging_dir, 'qmap', os.path.basename(qmap))
    output_source = os.path.join(dataset_staging_dir, 'output', qmap_name, dataset_name)

    filepaths = {
        "dataset": dataset_name,
        "staging_dir": dataset_staging_dir,
        "raw": {
            "source": deployment.source_collection.to_globus(input_source),
            "destination": deployment.staging_collection.to_globus(input_staging),
            "compute": raw_dest,
            "recursive": True,
        },
        "qmap": {
            "source": deployment.source_collection.to_globus(qmap),
            "destination": deployment.staging_collection.to_globus(qmap_dest),
            "compute": qmap_dest,
            "recursive": False
        },
        "output": {
            "source": deployment.staging_collection.to_globus(output_source),
            "destination": deployment.source_collection.to_globus(output) if output else "",
            "recursive": False,
            "compute": {
                "directory": output_source,
                # Note: This is how boost_corr names output files. It dumps a file named after the input in the
                # output directory provided
                "file": os.path.join(output_source, dataset_name + "_results.hdf")
            }
        },
        "plot": {
            "compute": os.path.join(output_source, "resources")
        },
        "publish": {
            "compute": {
                "source": output_source,
                "destination": os.path.join(deployment.pub_collection_basepath, cycle or determine_cycle(raw), experiment, qmap_name),
            },
            "metadata": os.path.join(output_source, "resources", dataset_name + "_results", "metadata.json")
        },
    }     
    return filepaths

def get_flow_input(
        deployment: BaseDeployment, 
        filepaths: dict,
        boost_corr: dict,
        skip_transfer_back: bool = False,
        additional_groups: list = None,
        extra_metadata: dict = None):
    """
    Generates input that can be used to start the XPCS Flow. 

    :param deployment: The deployment used in this flow. MUST be the same one used in get_filepaths()
    :param boost_corr: Parameters to pass to boost_corr directly.
    :param skip_transfer_back: Skip the transfer back to source.
    :param additional_groups: Any additional groups to give access to on publish
    :param extra_metadata: Any other static metadata that should be included when publishing this dataset
    """
    depl_input = deployment.get_input()
    visible_to = list(set(['urn:globus:groups:id:{g}' for g in additional_groups or []] + [DEVELOPER_GROUP]))
    extra_metadata = extra_metadata or dict()
    flow_input = {
        'input': {
            'boost_corr': boost_corr,
            'publishv2': {
                    'dataset': filepaths["publish"]["compute"]["source"],
                    'destination': filepaths["publish"]["compute"]["destination"],
                    'source_collection': deployment.staging_collection.uuid,
                    'source_collection_basepath': str(deployment.staging_collection.path),
                    'destination_collection': str(deployment.pub_collection.uuid),
                    # Deprecated, do not use for publishing. We may migrate the metadata in this index
                    # to the newer format used below
                    # 'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                    # Current "XPCS v2 index"
                    'index': '4428cfe6-611b-48db-81b2-167a7d9710ea',
                    'visible_to': visible_to,
                    # Ingest and Transfer can be disabled for dry-run testing.
                    'enable_publish': True,
                    'enable_transfer': True,

                    'enable_meta_dc': True,
                    'enable_meta_files': True,
                    # Metadata file will consist of all metadata the webplot generates
                    "metadata_file": filepaths["publish"]["metadata"],
                    # 'metadata_dc_validation_schema': 'schema43',
                    'destination_url_hostname': 'https://g-f6125.fd635.8443.data.globus.org',
                },

            'source_transfer': {
                'source_endpoint_id': deployment.source_collection.uuid,
                'destination_endpoint_id': deployment.staging_collection.uuid,
                'transfer_items': [
                    {
                        'source_path': filepaths['raw']['source'],
                        'destination_path': filepaths['raw']['destination'],
                        'recursive': filepaths['raw']['recursive'],
                    },
                    {
                        'source_path': filepaths['qmap']['source'],
                        'destination_path': filepaths['qmap']['destination'],
                        'recursive': filepaths['qmap']['recursive'],
                    },
                ],
            },

            'enable_result_transfer': not skip_transfer_back, 
            'result_transfer': {
                'source_endpoint_id': deployment.staging_collection.uuid,
                'destination_endpoint_id': deployment.source_collection.uuid,
                'transfer_items': [{
                    'source_path': filepaths['output']['source'],
                    'destination_path': filepaths['output']['destination'],
                    'recursive': filepaths['output']['recursive'],
                }],
            },
            'corr_results': filepaths['output']['compute']['file'], 
            'webplot_target_dir': filepaths['plot']['compute'],
            "webplot_extra_metadata": {
                "source_files": {
                    "raw": [
                        filepaths['raw']['source'],
                    ],
                    "qmap": [
                        filepaths['qmap']['source'],
                    ]
                },
                **extra_metadata,

            },
            'compute_endpoint': depl_input['input']['compute_endpoint'],
        }
    }
    if skip_transfer_back is False and not flow_input["input"]["result_transfer"]["transfer_items"][0]["destination_path"]:
        raise ValueError("Either set skip_transfer_back to True or provide a value for 'output'")

    return flow_input


def get_boost_corr_arguments(args, filepaths):
    """
    This function is a little lazy. It simply returns arguments in a way that satisfies the xpcs_boost_corr
    tool. However, it exists separately so it can be re-implemented without a dependency on the argument
    parser. This will be useful when the portal needs to do reprocessing.
    """
    return {
            "type": args.type,
            "qmap": filepaths['qmap']['compute'],
            "raw": filepaths['raw']['compute'],
            "output": filepaths['output']['compute']['directory'],
            "gpu_id": args.gpu_id,
            "verbose": args.verbose,
            "begin_frame": args.begin_frame,
            "end_frame": args.end_frame,
            "avg_frame": args.avg_frame,
            "stride_frame": args.stride_frame,
            "overwrite": args.overwrite,
            "dq_selection": args.dq_selection,
            "save_g2": args.save_g2,
            "smooth": args.smooth,
    }

def determine_experiment(experiment: str):
    """
    Attempts to extract common group names. Experiments typically look like
    milliron202503, with numbers that denote the cycle. This function removes
    the cycle info for common experiment names like "milliron" which can be
    used to group common experiments in the search index.
    """
    try:
        re.match("([a-zA-Z]+)\d*", experiment).groups()[0]
    except Exception:
        pass
    return experiment

def get_extra_metadata(experiment, cycle=None):
    """
    Fetch extra metadata to include in the publishing step.

    :param experiment: Used for grouping common experiments in publishing
    :param cycle: Used for grouping datasets by cycle
    """
    return {
        "experiment": determine_experiment(experiment),
        "cycle": cycle or determine_cycle(cycle)
    }
    
def globus_connection(func, *args, **kwargs):
    """
    Used if the connection isn't great at the APS. Sometimes this happens. Hopefully we can remove this in
    the future...
    """
    try:
        return func(*args, **kwargs)
    except GlobusConnectionError as e:
        print(f"Caught GlobusConnectionError during {func}. Retrying connection.")
        time.sleep(1)
        return globus_connection(func, *args, **kwargs)
    
def start_flow(flow_input: dict, dataset_name: str, args_experiment: str):
    """
    Start an XPCS flow.

    Requires flow input from get_flow_input().

    :param flow_input: Input generated from get_flow_input()
    :param dataset_name: A filename used for flow labels
    :param args_experiment: The name of the experiment, used to tag this run
    """
    corr_flow = XPCSBoost()
    print("Submitting flow to Globus...")
    flow_run = globus_connection(corr_flow.run_flow, flow_input=flow_input, label=dataset_name, tags=['aps', 'xpcs', args_experiment])
    print("Flow successfully submitted to Globus.")

    actionID = flow_run['action_id']
    print(f"Flow Action ID: {actionID}")
    print(f"URL: https://app.globus.org/runs/{actionID}")

    print("Getting flow status from Globus...")
    status = globus_connection(corr_flow.get_status, action_id=actionID).get('status')
    print(f"Status: {status}")

if __name__ == '__main__':
    args = arg_parse()
    deployment = get_deployment(args.deployment, args.raw)
    filepaths = get_filepaths(args.raw, args.qmap, args.output, args.experiment, deployment, cycle=args.cycle)
    metadata = get_extra_metadata(args.experiment, args.cycle or determine_cycle(args.raw))
    boost_corr = get_boost_corr_arguments(args, filepaths)
    flow_input = get_flow_input(deployment, filepaths, boost_corr, skip_transfer_back=args.skip_transfer_back, extra_metadata=metadata)

    from pprint import pprint
    pprint(flow_input)
    start_flow(flow_input, filepaths["dataset"], args.experiment)
