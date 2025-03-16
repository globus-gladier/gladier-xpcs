#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

import argparse
import os
import sys
import pathlib
import time
import traceback

from gladier_xpcs.flows import XPCSBoost
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


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment', help='Name of the DM experiment', default='comm202410')
    parser.add_argument('--hdf', help='Path to the hdf (metadata) file',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/G001_436_PorousGlass-08000/G001_436_PorousGlass-08000.hdf')
    parser.add_argument('-r', '--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/G001_436_PorousGlass-08000/G001_436_PorousGlass-08000.h5')
    parser.add_argument('-q', '--qmap', help='Path to the qmap file',
                        default='/gdata/dm/8IDI/2024-3/comm202410/data/eiger4m_qmap_1018_hongrui_d36.h5')
    parser.add_argument('-t', '--type', default='Multitau', help='Analysis type to be performed. Available: Multitau, Twotime')
    parser.add_argument('-i', '--gpu-id', type=int, default=0, help='''Choose which GPU to use. if the input is -1, then CPU is used''')
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

def globus_connection(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except GlobusConnectionError as e:
        print(f"Caught GlobusConnectionError during {func}. Retrying connection.")
        time.sleep(1)
        return globus_connection(func, *args, **kwargs)

def add_rigaku_transfer_items(flow_input, args_raw, raw_file):
    ''' Rigaku detector splits raw data into 6 separate files
        with extension .bin.000 - .bin.005 
        Boost corr takes the 000 file as argument and then looks for the other 5
        This function adds the other 5 file names to the list of source transfer items  
    ''' 
    if raw_file.endswith('000'):
        # remove last 0
        args_raw = args_raw[:-1]
        raw_file = raw_file[:-1]
        for i in range(1, 6):
            transfer_item = {
                'source_path': deployment.source_collection.to_globus(f"{args_raw}{i}"),
                'destination_path': deployment.staging_collection.to_globus(f"{raw_file}{i}"),
            }
            flow_input['input']['source_transfer']['transfer_items'].append(transfer_item)
    return flow_input
    
if __name__ == '__main__':
    args = arg_parse()
    print(args)
    deployment = deployment_map.get(args.deployment)
    if "/gdata/dm/XPCS8" in args.raw:
        deployment = deployment_map.get('voyager-xpcs8-polaris')
    if not deployment:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')
    elif deployment.service_account and not (os.getenv('GLADIER_CLIENT_ID') and os.getenv('GLADIER_CLIENT_SECRET')):
        raise ValueError(f'Deployment requires setting GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET')

    atype_options = ['Multitau', 'Both', 'Twotime']
    if args.type not in atype_options:
        raise ValueError(f'Invalid --type, must be one of: {", ".join(atype_options)}')

    depl_input = deployment.get_input()

    raw_name = os.path.basename(args.raw)
    hdf_name = os.path.basename(args.hdf)
    print(f"{hdf_name=}")
    qmap_name = os.path.basename(args.qmap)
    dataset_name = raw_name[:raw_name.rindex('.')] #remove file extension
    print(f"{dataset_name=}")
    dataset_dir = os.path.join(depl_input['input']['staging_dir'], args.experiment, dataset_name)
    print(f"{dataset_dir=}")
    #Processing type
    atype = args.type

    # Generate Source Pathnames.
    #source_raw_path = os.path.join(args.experiment, 'data', args.raw)
    #source_qmap_path = os.path.join(args.experiment, 'data', args.qmap)
    #Output path to transfer results back
    # Generate Destination Pathnames.
    raw_file = os.path.join(dataset_dir, 'input', raw_name)
    qmap_file = os.path.join(dataset_dir, 'qmap', qmap_name)
    #do need to transfer the metadata file because corr will look for it
    #internally even though it is not specified as an argument
    input_hdf_file = os.path.join(dataset_dir, 'input', hdf_name)
    output_hdf_file = os.path.join(dataset_dir, 'output', dataset_name + "_results.hdf")
    # Required by boost_corr to know where to stick the output HDF
    output_dir = os.path.join(dataset_dir, 'output')
    # This tells the corr state where to place version specific info
    execution_metadata_file = os.path.join(dataset_dir, 'execution_metadata.json')

    if not args.skip_transfer_back:
        result_path_destination_filename = os.path.join(args.output, dataset_name + "_results.hdf")
        # Transfer back step transfers data to the following location automatically:
        #   /cycle/parent/analysis/dataset-name/dataset.hdf
        # Input dirs tend to look like the following, but the strongest convention we have is that the .hdf file
        # will be within a directory of the same name. It *may* be in a 'data' directory, and if so, we want to
        # make sure processed data does not go back into the 'data' directory. Example paths look like this:
        #   /2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000/H001_27445_QZ_XPCS_test-01000.hdf

        print(
            f"Flow will transfer processed dataset {dataset_name} back to "
            f"{deployment.source_collection.name} ({deployment.source_collection.uuid}) with path "
            f"'{str(result_path_destination_filename)}'"
            )
        result_path_transfer_items = [
            {
                'source_path': deployment.staging_collection.to_globus(output_hdf_file),
                'destination_path': deployment.source_collection.to_globus(result_path_destination_filename)
            }
        ]
    else:
        print("--skip-transfer-back option was used, result will not be transferred back to source.")
        result_path_destination_filename = None
        result_path_transfer_items = []
    developerGroup = 'urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670'
    flow_input = {
        'input': {
            'boost_corr': {
                    "type": atype,
                    "qmap": qmap_file,
                    "raw": raw_file,
                    "output": output_dir,
                    # "batch_size": 8,  # No longer supported
                    "gpu_id": args.gpu_id,
                    "verbose": args.verbose,
                    # "masked_ratio_threshold": 0.75,  # No longer supported
                    # "use_loader": True,  # No longer supported
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
                    'dataset': dataset_dir,
                    'destination': '/XPCSDATA/Automate/',
                    'source_collection': deployment.staging_collection.uuid,
                    'source_collection_basepath': str(deployment.staging_collection.path),
                    'destination_collection': str(deployment.pub_collection.uuid),
                    'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                    # Test index
                    # 'index': '2ec9cf61-c0c9-4213-8f1c-452c072c4ccc',
                    'visible_to': [f'urn:globus:groups:id:{args.group}', developerGroup] if args.group else [developerGroup],

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
                        'source_path': deployment.source_collection.to_globus(args.raw),
                        'destination_path': deployment.staging_collection.to_globus(raw_file),
                    },
                    {
                        'source_path': deployment.source_collection.to_globus(args.hdf),
                        'destination_path': deployment.staging_collection.to_globus(input_hdf_file),
                    },
                    {
                        'source_path': deployment.source_collection.to_globus(args.qmap),
                        'destination_path': deployment.staging_collection.to_globus(qmap_file),
                    }
                ],
            },

            'enable_result_transfer': bool(result_path_destination_filename),
            'result_transfer': {
                'source_endpoint_id': deployment.staging_collection.uuid,
                'destination_endpoint_id': deployment.source_collection.uuid,
                'transfer_items': result_path_transfer_items,
            },
            'proc_dir': dataset_dir,
            'metadata_file': input_hdf_file,
            'hdf_file': output_hdf_file,
            'execution_metadata_file': execution_metadata_file,

            # globus compute endpoints
            'login_node_endpoint': depl_input['input']['login_node_endpoint'],
            'compute_endpoint': depl_input['input']['compute_endpoint'],
        }
    }
    add_rigaku_transfer_items(flow_input, args.raw, raw_file)

    corr_flow = XPCSBoost()

    corr_run_label = dataset_name
   
    print("Submitting flow to Globus...")
    flow_run = globus_connection(corr_flow.run_flow, flow_input=flow_input, label=corr_run_label, tags=['aps', 'xpcs', args.experiment])
    print("Flow successfully submitted to Globus.")

    actionID = flow_run['action_id']
    print(f"Flow Action ID: {actionID}")
    print(f"URL: https://app.globus.org/runs/{actionID}")

    print("Getting flow status from Globus...")
    status = globus_connection(corr_flow.get_status, action_id=actionID).get('status')
    print(f"Status: {status}")