#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

# Enable Gladier Logging
#import gladier.tests

import argparse
import os

from gladier_xpcs.flow_online import XPCSOnlineFlow
from gladier_xpcs.deployments import deployment_map


def register_container():
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_xpcs.tools.corr import eigen_corr
    cont_dir = '/eagle/APSDataAnalysis/XPCS/containers/'
    container_name = 'eigen_v2.simg'
    eigen_cont_id = fxc.register_container(location=cont_dir+container_name,container_type='singularity')
    corr_cont_fxid = fxc.register_function(eigen_corr, container_uuid=eigen_cont_id)
    return corr_cont_fxid


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', help='Path to the hdf file',
                        default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('--imm', help='Path to the imm',
                        default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                                '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--group', help='Visibility in Search', default=None)
    parser.add_argument('--deployment','-d', default='talc-prod', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
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

            'proc_dir': dataset_dir,
            'hdf_file': hdf_file,
            'imm_file': imm_file,
            'corr_loc': 'corr',
            'flags': '',

            # funcX endpoints
            # Should think of moving those to a cfg with better naming
            'funcx_endpoint_non_compute': depl_input['input']['funcx_endpoint_non_compute'],
            'funcx_endpoint_compute': depl_input['input']['funcx_endpoint_compute'],


            # globus endpoints
            'globus_endpoint_clutch': depl_input['input']['globus_endpoint_source'],
            'globus_endpoint_theta': depl_input['input']['globus_endpoint_proc'],

            # container hack for corr 
            'eigen_corr_funcx_id': register_container()
        }
    }


    corr_cli = XPCSOnlineFlow()

    corr_flow_label = hdf_name + '_dev'

    corr_flow = corr_cli.run_flow(flow_input=flow_input, label=corr_flow_label)

    print('run_id : ' + corr_flow['action_id'])

    #import pprint
    # pprint.pprint(flow_input)
    # pprint.pprint(corr_cli.flow_definition)
    # corr_cli.progress(corr_flow['action_id'])
    # pprint.pprint(corr_cli.get_status(corr_flow['action_id']))


    import argparse
    import time
    import sys
    from pprint import pprint

    from gladier.utils.flow_generation import get_ordered_flow_states
    flow_dict = get_ordered_flow_states(corr_cli.flow_definition)
    flow_steps = []
    for key, value in flow_dict.items() :
        flow_steps.append(key)
    
    if 'EigenCorr' not in flow_steps:
        print('EigenCorr' + ' not in valid steps')

    step_index = flow_steps.index('EigenCorr')
    status = corr_cli.get_status(corr_flow['action_id'])
    
    while status['status'] not in ['SUCCEEDED', 'FAILED']:

        status = corr_cli.get_status(corr_flow['action_id'])

        if status.get('state_name'):
            curr_step = status.get('state_name')       
        elif status.get('details'):
            det = status.get('details')
            if det.get('details'):
                curr_step = status['details']['details']['state_name']
            elif det.get('action_statuses'):
                curr_step = status['details']['action_statuses'][0]['state_name']
       
        print(curr_step)
        curr_index = flow_steps.index(curr_step)
        print(curr_index)
        
        if status['status']=='FAILED': #this could be out of the loop to prevent overchecking
            print('I have Failed!!')

        if curr_index>step_index:
            print('I already Passed Here!!')

        time.sleep(2)
