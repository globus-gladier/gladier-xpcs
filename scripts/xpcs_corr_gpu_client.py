#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

# Enable Gladier Logging
#import gladier.tests

import argparse
import os
import pathlib

from gladier_xpcs.flow_gpu import XPCSGPUFlow
from gladier_xpcs.deployments import deployment_map


def arg_parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('--hdf', help='Path to the hdf (metadata) file',
                        default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                                '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--qmap', help='Path to the qmap file',
                        default='/data/xpcs8/partitionMapLibrary/2019-1/comm201901_qmap_aerogel_Lq0.h5')
    parser.add_argument('--group', help='Visibility in Search', default=None)
    parser.add_argument('--deployment','-d', default='hannah-polaris', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
    parser.add_argument('--corr_gpu_loc', default='/eagle/projects/APSDataAnalysis/XPCS/mchu/xpcs_boost/gpu_corr.py',
                        help=f'Location of gpu corr processing script')
    parser.add_argument('--batch_size', default='256', help=f'Size of gpu corr processing batch')
    parser.add_argument('--verbose', default=False, action='store_true', help=f'Verbose output')

    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()

    depl = deployment_map.get(args.deployment)
    if not depl:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')

    depl_input = depl.get_input()

    raw_name = os.path.basename(args.raw)
    hdf_name = os.path.basename(args.hdf)
    qmap_name = os.path.basename(args.qmap)
    dataset_name = hdf_name[:hdf_name.rindex('.')] #remove file extension

    dataset_dir = os.path.join(depl_input['input']['staging_dir'], dataset_name)

    # Generate Destination Pathnames.
    raw_file = os.path.join(dataset_dir, 'input', raw_name)
    qmap_file = os.path.join(dataset_dir, 'qmap', qmap_name)
    #do need to transfer the metadata file because corr will look for it
    #internally even though it is not specified as an argument
    input_hdf_file = os.path.join(dataset_dir, 'input', hdf_name)
    output_hdf_file = os.path.join(dataset_dir, 'output', hdf_name)

    flow_input = {
        'input': {
            'pilot': {
                # This is the directory which will be published
                'dataset': dataset_dir,
                # Old index, switch back to this when we want to publish to the main index
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                # Test Index, use this for testing
                # 'index': '2e72452f-e932-4da0-b43c-1c722716896e',
                'project': 'xpcs-8id',
                'source_globus_endpoint': depl_input['input']['globus_endpoint_proc'],
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': [args.group] if args.group else [],
                'metadata': {'executable_name': 'corr_gpu',
                             'executable_version': 'prototype'}
            },

            'transfer_from_clutch_to_theta_items': [
                {
                    'source_path': args.raw,
                    'destination_path': raw_file,
                },
                {
                    'source_path': args.hdf,
                    'destination_path': input_hdf_file,
                },
                {
                    'source_path': args.qmap,
                    'destination_path': qmap_file,
                }
            ],

            'proc_dir': dataset_dir,
            'raw_file': raw_file,
            'qmap_file': qmap_file,
            'metadata_file': input_hdf_file,
            'hdf_file': output_hdf_file,
            'corr_gpu_loc': args.corr_gpu_loc,
            'batch_size': args.batch_size,
            'verbose': args.verbose,

            # funcX endpoints
            'funcx_endpoint_non_compute': depl_input['input']['funcx_endpoint_non_compute'],
            'funcx_endpoint_compute': depl_input['input']['funcx_endpoint_compute'],

            # globus endpoints
            'globus_endpoint_clutch': depl_input['input']['globus_endpoint_source'],
            'globus_endpoint_theta': depl_input['input']['globus_endpoint_proc'],
        }
    }

    corr_flow = XPCSGPUFlow()
    corr_run_label = pathlib.Path(hdf_name).name[:62]
    flow_run = corr_flow.run_flow(flow_input=flow_input, label=corr_run_label)

    print('run_id : ' + flow_run['action_id'])