#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

# Enable Gladier Logging
import gladier.tests

from gladier_xpcs.online_processing import XPCSClient
import argparse
import os
import pprint


# This is a patch to continue using funcx 0.0.3 until the new AP comes online.
def register_container():
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_xpcs.tools.corr import eigen_corr
    cont_dir = '/eagle/APSDataAnalysis/XPCS_test/containers/'
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
    parser.add_argument('--source-globus-ep', help='Source Globus Endpoint (Default Clutch)',
                        default='fdc7e74a-fa78-11e8-9342-0e3d676669f4')
    parser.add_argument('--compute-globus-ep', help='Compute Globus Endpoint (Default Theta)',
                        default='08925f04-569f-11e7-bef8-22000b9a448b')
    parser.add_argument('--processing-dir', help='Location folder on compute endpoint to process data',
                        default='/eagle/APSDataAnalysis/XPCS_test')

    return parser.parse_args()


if __name__ == '__main__':

    args = arg_parse()
    hdf_name = os.path.basename(args.hdf)
    imm_name = os.path.basename(args.imm)

    input_parent_dir = hdf_name.replace('.hdf', '')
    dataset_dir = os.path.join(args.processing_dir, input_parent_dir)

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
                'source_globus_endpoint': args.compute_globus_ep,
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
            'funcx_endpoint_non_compute':'e449e8b8-e114-4659-99af-a7de06feb847',
            'funcx_endpoint_compute':    '4c676cea-8382-4d5d-bc63-d6342bdb00ca',
            #'funcx_endpoint_non_compute': '8f2f2eab-90d2-45ba-a771-b96e6d530cad',
            #'funcx_endpoint_compute':     '9337a3c3-0ee5-45b8-bcbd-8a277f461e23',

            # globus endpoints
            'globus_endpoint_clutch': args.source_globus_ep,
            'globus_endpoint_theta': args.compute_globus_ep,

            # container hack for corr 
            'eigen_corr_funcx_id': register_container()
        }
    }


    corr_cli = XPCSClient()

    corr_flow = corr_cli.run_flow(flow_input=flow_input)

    #print(corr_cli.get_flow_id)
    print(corr_flow['action_id'])

    # pprint.pprint(flow_input)
    # pprint.pprint(corr_cli.flow_definition)
    # corr_cli.progress(corr_flow['action_id'])
    # pprint.pprint(corr_cli.get_status(corr_flow['action_id']))
