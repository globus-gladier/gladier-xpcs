#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

# Enable Gladier Logging
#import gladier.tests

import argparse
import os
import pathlib

from gladier_xpcs.flows import XPCSEigen
from gladier_xpcs.deployments import deployment_map


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', help='Path to the hdf file',
                        default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('--imm', help='Path to the imm',
                        default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                                '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--group', help='Visibility in Search', default=None)
    parser.add_argument('--deployment','-d', default='nick-polaris', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
    parser.add_argument('--results', default='/data/xpcs8/2019-1/comm201901/ALCF_results/',
                        help=f'Clutch transfer location for corr results on hdf file')
    return parser.parse_args()


if __name__ == '__main__':

    args = arg_parse()

    depl = deployment_map.get(args.deployment)
    if not depl:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')

    depl_input = depl.get_input()

    hdf_name = os.path.basename(args.hdf)
    imm_name = os.path.basename(args.imm)

    input_parent_dir = hdf_name.replace('.hdf', '')
    dataset_dir = os.path.join(depl_input['input']['staging_dir'], input_parent_dir)

    # Generate Destination Pathnames.
    hdf_file = os.path.join(dataset_dir, hdf_name)
    imm_file = os.path.join(dataset_dir, imm_name)
    hdf_file_result = os.path.join(args.results, hdf_name)

    flow_input = {
        'input': {
            'pilot': {
                # This is the directory which will be published to petrel
                'dataset': dataset_dir,
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                'project': 'xpcs-8id',
                'source_globus_endpoint': depl_input['input']['globus_endpoint_proc'],
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': [args.group] if args.group else [],
            },

            'transfer_from_clutch_to_theta_items': [
                {
                    'source_path': args.hdf,
                    'destination_path': hdf_file,
                },
                {
                    'source_path': args.imm,
                    'destination_path': imm_file,
                }
            ],

            'transfer_to_clutch': [
                {
                    'source_path': hdf_file,
                    'destination_path': hdf_file_result,
                },
            ],

            'proc_dir': dataset_dir,
            'hdf_file': hdf_file,
            'imm_file': imm_file,
            'corr_loc': 'corr',
            'flags': '',

            # globus compute endpoints
            # Should think of moving those to a cfg with better naming
            'compute_endpoint_non_compute': depl_input['input']['compute_endpoint_non_compute'],
            'compute_endpoint_compute': depl_input['input']['compute_endpoint_compute'],


            # globus endpoints
            'globus_endpoint_clutch': depl_input['input']['globus_endpoint_source'],
            'globus_endpoint_theta': depl_input['input']['globus_endpoint_proc'],
        }
    }

    corr_flow = XPCSEigen()

    corr_run_label = pathlib.Path(hdf_name).name[:62]
    flow_run = corr_flow.run_flow(flow_input=flow_input, label=corr_run_label)
    print('run_id : ' + flow_run['action_id'])


