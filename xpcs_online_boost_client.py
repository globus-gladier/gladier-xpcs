#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

import argparse
import os
import pathlib
import time

from gladier_xpcs.flows import XPCSBoost
from gladier_xpcs.deployments import deployment_map
from gladier_xpcs import log  # noqa Add INFO logging

from globus_sdk import ConfidentialAppAuthClient, AccessTokenAuthorizer
from gladier.managers.login_manager import CallbackLoginManager

from typing import List, Mapping, Union
import traceback
from fair_research_login import JSONTokenStorage

# Get client id/secret
CLIENT_ID = os.getenv("GLADIER_CLIENT_ID")
CLIENT_SECRET = os.getenv("GLADIER_CLIENT_SECRET")


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--experiment', help='Name of the DM experiment', default='test-xpcs-local-workflow-2023.12.19-01')
    parser.add_argument('--hdf', help='Path to the hdf (metadata) file',
                        default='A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('-r', '--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('-q', '--qmap', help='Path to the qmap file',
                        default='comm201901_qmap_aerogel_Lq0.h5')
    parser.add_argument('-t', '--atype', default='Both', help='Analysis type to be performed. Available: Multitau, Twotime')
    parser.add_argument('-i', '--gpu_flag', type=int, default=0, help='''Choose which GPU to use. if the input is -1, then CPU is used''')
    # Group MUST not be None in order for PublishTransferSetPermission to succeed. Group MAY
    # be specified even if the flow owner does not have a role to set ACLs, in which case PublishTransferSetPermission will be skipped.
    parser.add_argument('--group', help='Visibility in Search', default='368beb47-c9c5-11e9-b455-0efb3ba9a670')
    parser.add_argument('--deployment','-d', default='voyager-8idi-polaris', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
    parser.add_argument('--batch_size', default='256', help=f'Size of gpu corr processing batch')
    parser.add_argument('-v', '--verbose', default=False, action='store_true', help=f'Verbose output')
    parser.add_argument('-o', '--output_dir', help=f'Output directory')
    parser.add_argument('-s', '--smooth', default='sqmap', help=f'Smooth method to be used in Twotime correlation.')
    parser.add_argument('--save_G2', default=False, action='store_true', help=f'Save G2, IP, and IF to file.')
    parser.add_argument('-avg_frame', '--avgFrame', default=1, type=int, help=f'Defines the number of frames to be averaged before the correlation.')
    parser.add_argument('-begin_frame', '--beginFrame', default=1, type=int, help=f'Specifies which frame to begin with for the correlation. ')
    parser.add_argument('-end_frame', '--endFrame', default=-1, type=int, help=f'Specifies the last frame used for the correlation.')
    parser.add_argument('-stride_frame', '--strideFrame', default=1, type=int, help=f'Defines the stride.')
    parser.add_argument('-ow', '--overwrite', default=False, action='store_true', help=f'Overwrite the existing result file.')
    parser.add_argument('-dq', '--dq', default='all', help=f'A string that selects the dq list, eg. \'1, 2, 5-7\' selects [1,2,5,6,7]')
    return parser.parse_args()


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
    if args.atype not in atype_options:
        raise ValueError(f'Invalid --atype, must be one of: {", ".join(atype_options)}')

    depl_input = deployment.get_input()

    raw_name = os.path.basename(args.raw)
    hdf_name = os.path.basename(args.hdf)
    print(f"{hdf_name=}")
    qmap_name = os.path.basename(args.qmap)
    dataset_name = hdf_name[:hdf_name.rindex('.')] #remove file extension
    print(f"{dataset_name=}")
    dataset_dir = os.path.join(depl_input['input']['staging_dir'], args.experiment, dataset_name)
    print(f"{dataset_dir=}")
    #Processing type
    atype = args.atype

    # Generate Source Pathnames.
    #source_raw_path = os.path.join(args.experiment, 'data', args.raw)
    #source_qmap_path = os.path.join(args.experiment, 'data', args.qmap)
    #Output path to transfer results back
    result_path = os.path.join(args.output_dir, hdf_name)
    # Generate Destination Pathnames.
    raw_file = os.path.join(dataset_dir, 'input', raw_name)
    qmap_file = os.path.join(dataset_dir, 'qmap', qmap_name)
    #do need to transfer the metadata file because corr will look for it
    #internally even though it is not specified as an argument
    input_hdf_file = os.path.join(dataset_dir, 'input', hdf_name)
    output_hdf_file = os.path.join(dataset_dir, 'output', hdf_name)
    # Required by boost_corr to know where to stick the output HDF
    output_dir = os.path.join(dataset_dir, 'output')
    # This tells the corr state where to place version specific info
    execution_metadata_file = os.path.join(dataset_dir, 'execution_metadata.json')

    flow_input = {
        'input': {

            'boost_corr': {
                    'atype': atype,
                    "qmap": qmap_file,
                    "raw": raw_file,
                    "output": output_dir,
                    "batch_size": 8,
                    "gpu_id": args.gpu_flag,
                    "verbose": args.verbose,
                    "masked_ratio_threshold": 0.75,
                    "use_loader": True,
                    "begin_frame": args.beginFrame,
                    "end_frame": args.endFrame,
                    "avg_frame": args.avgFrame,
                    "stride_frame": args.strideFrame,
                    "overwrite": args.overwrite,
                    "dq": args.dq,
                    "save_G2": args.save_G2,
                    "smooth": args.smooth,
            },

            'pilot': {
                # This is the directory which will be published
                'dataset': dataset_dir,
                # Old index, switch back to this when we want to publish to the main index
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                # Test Index, use this for testing
                # 'index': '2e72452f-e932-4da0-b43c-1c722716896e',
                'project': 'xpcs-8id',
                'source_globus_endpoint': depl_input['input']['globus_endpoint_proc'],
                'source_collection_basepath': str(deployment.staging_collection.path),
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': [args.group] if args.group else [],
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

            'result_transfer': {
                'source_endpoint_id': deployment.staging_collection.uuid,
                'destination_endpoint_id': deployment.source_collection.uuid,
                'transfer_items': [
                    {
                        'source_path': deployment.staging_collection.to_globus(output_hdf_file),
                        'destination_path': deployment.source_collection.to_globus(result_path)
                    }
                ]
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

    corr_flow = XPCSBoost()

    corr_run_label = pathlib.Path(hdf_name).name[:62]
    flow_run = corr_flow.run_flow(flow_input=flow_input, label=corr_run_label, tags=['aps', 'xpcs'])

    actionID = flow_run['action_id']
    print(f"Flow Action ID: {actionID}")
    print(f"URL: https://app.globus.org/runs/{actionID}")
    status = corr_flow.get_status(actionID).get('status')
    print(f"Status: {status}")