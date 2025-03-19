#!/dmOpt/workflows/miniconda3/envs/gladier-0.9.4/bin/python

## /dmOpt/workflows/miniconda3/envs/gladier-0.9.4/bin/python /home/dm/workflows/xpcs8/gladier-xpcs/scripts/xpcs_online_boost_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

import argparse
import os
import sys
import pathlib
import time
import traceback

from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier_xpcs.deployments import deployment_map
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
    parser.add_argument('--hdf', help='Path to the hdf (metadata) file',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/G001_436_PorousGlass-08000/G001_436_PorousGlass-08000.hdf')
    parser.add_argument('-r', '--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/G001_436_PorousGlass-08000/G001_436_PorousGlass-08000.h5')
    parser.add_argument('-q', '--qmap', help='Path to the qmap file',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/eiger4m_qmap_1018_hongrui_d36.h5')
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
    parser.add_argument('-w', '--overwrite', default=False, action='store_true', help=f'Overwrite the existing result file.')
    parser.add_argument('-d', '--dq-selection', default='all', help=f'A string that selects the dq list, eg. \'1, 2, 5-7\' selects [1,2,5,6,7]')
    parser.add_argument('-o', '--output', help=f'Output directory')

    return parser.parse_args()

def get_deployment(args_deployment, args_raw):
    deployment = deployment_map.get(args_deployment)
    if "/gdata/dm/XPCS8" in args_raw:
        deployment = deployment_map.get('voyager-xpcs8-polaris')
    if not deployment:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')
    elif deployment.service_account and not (os.getenv('GLADIER_CLIENT_ID') and os.getenv('GLADIER_CLIENT_SECRET')):
        raise ValueError(f'Deployment requires setting GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET')
    return deployment

def get_filepaths(raw, hdf, qmap, output, experiment, deployment):
    ''' Generate File Pathnames
        do need to transfer the metadata file because corr will look for it
        internally even though it is not specified as an argument
    '''
    depl_input = deployment.get_input()
    dataset_name = os.path.basename(raw).split('.')[0] #raw_name[:raw_name.rindex('.')] #remove file extension
    dataset_staging_dir = os.path.join(depl_input['input']['staging_dir'], experiment, dataset_name)

    raw_dest = os.path.join(dataset_staging_dir, 'input', os.path.basename(raw))
    qmap_dest = os.path.join(dataset_staging_dir, 'qmap', os.path.basename(qmap))
    hdf_dest = os.path.join(dataset_staging_dir, 'input', os.path.basename(hdf))
    output_source = os.path.join(dataset_staging_dir, 'output')
    # This tells the corr state where to place version specific info
    execution_metadata_file = os.path.join(dataset_staging_dir, 'execution_metadata.json')

    filepaths = {
        "raw": {
            "source": deployment.source_collection.to_globus(raw),
            "destination": deployment.staging_collection.to_globus(raw_dest),
            "compute": raw_dest
        },
        "metadata": {
            "source": deployment.source_collection.to_globus(hdf),
            "destination": deployment.staging_collection.to_globus(hdf_dest),
            "compute": hdf_dest
        },
        "qmap": {
            "source": deployment.source_collection.to_globus(qmap),
            "destination": deployment.staging_collection.to_globus(qmap_dest),
            "compute": qmap_dest
        },
        "output": {
            "source": deployment.staging_collection.to_globus(output_source),
            "destination": deployment.source_collection.to_globus(output),
            "compute": {
                "directory": output_source,
                "file": os.path.join(output_source, dataset_name + "_results.hdf")
            }
        },
        "execution_metadata_file": execution_metadata_file
    }     
    return filepaths, dataset_name, dataset_staging_dir

def get_flow_input(args, deployment, filepaths, dataset_staging_dir):
    depl_input = deployment.get_input()
    flow_input = {
        'input': {
            'boost_corr': {
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
            },
            'publishv2': {
                    'dataset': dataset_staging_dir, 
                    'destination': '/XPCSDATA/Automate/',
                    'source_collection': deployment.staging_collection.uuid,
                    'source_collection_basepath': str(deployment.staging_collection.path),
                    'destination_collection': str(deployment.pub_collection.uuid),
                    'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                    # Test index
                    # 'index': '2ec9cf61-c0c9-4213-8f1c-452c072c4ccc',
                    'visible_to': [f'urn:globus:groups:id:{args.group}', DEVELOPER_GROUP] if args.group else [DEVELOPER_GROUP],

                    # Ingest and Transfer can be disabled for dry-run testing.
                    'enable_publish': True,
                    'enable_transfer': True,

                    'enable_meta_dc': True,
                    'enable_meta_files': True,
                    # Use this to validate the 'dc' or datacite field metadata schema
                    # Requires 'datacite' package
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
                    },
                    {
                        'source_path': filepaths['qmap']['source'],
                        'destination_path': filepaths['qmap']['destination'],
                    },
                    {
                        'source_path': filepaths['metadata']['source'],
                        'destination_path': filepaths['metadata']['destination'],
                    }
                ],
            },

            'enable_result_transfer': not args.skip_transfer_back, 
            'result_transfer': {
                'source_endpoint_id': deployment.staging_collection.uuid,
                'destination_endpoint_id': deployment.source_collection.uuid,
                'transfer_items': [],
            },
            'proc_dir': dataset_staging_dir,
            'metadata_file': filepaths['metadata']['compute'], 
            'hdf_file': filepaths['output']['compute']['file'], 
            'execution_metadata_file': filepaths['execution_metadata_file'],

            # globus compute endpoints
            'login_node_endpoint': depl_input['input']['login_node_endpoint'],
            'compute_endpoint': depl_input['input']['compute_endpoint'],
        }
    }
    flow_input = add_result_transfer_items(flow_input, filepaths)
    flow_input = add_rigaku_transfer_items(flow_input, filepaths['raw'], deployment)
    return flow_input

def add_result_transfer_items(flow_input, filepaths):
    ''' Transfer back step transfers data to the following location automatically:
        /cycle/parent/analysis/dataset-name/dataset.hdf
        Input dirs tend to look like the following, but the strongest convention we have is that the .hdf file
        will be within a directory of the same name. It *may* be in a 'data' directory, and if so, we want to
        make sure processed data does not go back into the 'data' directory. Example paths look like this:
        /2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000/H001_27445_QZ_XPCS_test-01000.hdf
    '''
    if not args.skip_transfer_back:
        print(
            f"Flow will transfer processed dataset {dataset_name} back to "
            f"{deployment.source_collection.name} ({deployment.source_collection.uuid}) with path "
            f"'{filepaths['output']['destination']}'"
            )

        result_path_transfer_item = {
            'source_path': filepaths['output']['source'],
            'destination_path': filepaths['output']['destination']
        }
        flow_input['input']['result_transfer']['transfer_items'].append(result_path_transfer_item)
    else:
        print("--skip-transfer-back option was used, result will not be transferred back to source.")
    return flow_input

def add_rigaku_transfer_items(flow_input, raw_filepaths, deployment):
    ''' Rigaku detector splits raw data into 6 separate files
        with extension .bin.000 - .bin.005 
        Boost corr takes the 000 file as argument and then looks for the other 5
        This function adds the other 5 file names to the list of source transfer items  
    ''' 
    raw_dest = raw_filepaths['destination']
    raw_source = raw_filepaths['source']
    if raw_dest.endswith('000'):
        # remove last 0
        raw_source = raw_source[:-1]
        raw_dest = raw_dest[:-1]
        for i in range(1, 6):
            transfer_item = {
                'source_path': f"{raw_source}{i}",
                'destination_path': f"{raw_dest}{i}",
            }
            flow_input['input']['source_transfer']['transfer_items'].append(transfer_item)
    return flow_input
    
def globus_connection(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except GlobusConnectionError as e:
        print(f"Caught GlobusConnectionError during {func}. Retrying connection.")
        time.sleep(1)
        return globus_connection(func, *args, **kwargs)
    
def start_flow(flow_input, dataset_name, args_experiment):
    corr_flow = XPCSBoost()
    corr_run_label = dataset_name
   
    print("Submitting flow to Globus...")
    flow_run = globus_connection(corr_flow.run_flow, flow_input=flow_input, label=corr_run_label, tags=['aps', 'xpcs', args_experiment])
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
    filepaths, dataset_name, dataset_staging_dir = get_filepaths(args.raw, args.hdf, 
        args.qmap, args.output, args.experiment, deployment)
    flow_input = get_flow_input(args, deployment, filepaths, dataset_staging_dir)
    print(flow_input)
    start_flow(flow_input, dataset_name, args.experiment)
