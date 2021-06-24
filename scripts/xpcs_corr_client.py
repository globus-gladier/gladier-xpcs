#!/usr/bin/env python 


##

## raw_hdf = '/net/wolf/data/xpcs8/2021-1/leheny202101/A010_00001_Vol20_quenchT102p7ohms_att1_Rq0/A010_00001_Vol20_quenchT102p7ohms_att1_Rq0_0001-100000.hdf'

# /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py
#--hdf /net/wolf/data/xpcs8/2021-1/leheny202101/cluster_results/A010_00001_Vol20_quenchT102p7ohms_att1_Rq0_0001-100000.hdf 
# fileDataDir:A010_00001_Vol20_quenchT102p7ohms_att1_Rq0 


# Import Gladier base
from gladier import GladierBaseClient, generate_flow_definition
# Enable Gladier Logging
import gladier.tests

import argparse
import os
import pprint


@generate_flow_definition
class XPCSClient(GladierBaseClient):
    gladier_tools = [
        'gladier_xpcs.tools.transfer_from_clutch_to_theta.TransferFromClutchToTheta',
        'gladier_xpcs.tools.pre_publish.PrePublish',
        'gladier_xpcs.tools.EigenCorr',
        # 'gladier_xpcs.tools.transfer_from_clutch_to_theta.TransferToClutch',
        'gladier_xpcs.tools.plot.MakeCorrPlots',
        'gladier_xpcs.tools.gather_xpcs_metadata.GatherXPCSMetadata',
        'gladier_tools.publish.Publish',
    ]


# This is a patch to continue using funcx 0.0.3 until the new AP comes online.
def register_container():
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_xpcs.tools.corr import eigen_corr
    # cont_dir = '/eagle/projects/APSDataAnalysis/XPCS/containers/'
    cont_dir = '/eagle/projects/APSDataAnalysis/XPCS_test/containers/'
    container_name = 'eigen.simg'
    eigen_cont_id = fxc.register_container(location=cont_dir+'/'+container_name,container_type='singularity')
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
    parser.add_argument('--source-endpoint', help='Source endpoint (Default Clutch)',
                        default='fdc7e74a-fa78-11e8-9342-0e3d676669f4')
    parser.add_argument('--compute-endpoint', help='Compute endpoint (Default Eagle)',
                        default='05d2c76a-e867-4f67-aa57-76edeb0beda0')
    parser.add_argument('--processing-dir', help='Location folder on compute endpoint to process data',
                        default='/eagle/projects/APSDataAnalysis/XPCS_test/')

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
                'source_globus_endpoint': args.compute_endpoint,
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

            'proc_dir': args.processing_dir,
            'hdf_file': hdf_file,
            'imm_file': imm_file,
            'corr_loc': '/bin/echo',
            'flags': '',

            # funcX endpoints
            'funcx_endpoint_non_compute': '7b08f56f-7426-4c82-ae1a-c299ec1e673c',
            'funcx_endpoint_compute': '7b08f56f-7426-4c82-ae1a-c299ec1e673c',

            # globus endpoints
            'globus_endpoint_clutch': args.source_endpoint,
            'globus_endpoint_theta': args.compute_endpoint,

            # container hack for corr 
            # 'eigen_corr_fxid': register_container()
        }
    }

    corr_cli = XPCSClient()
    pprint.pprint(flow_input)
    pprint.pprint(corr_cli.flow_definition)
    corr_flow = corr_cli.run_flow(flow_input=flow_input)
    corr_cli.progress(corr_flow['action_id'])
    pprint.pprint(corr_cli.get_status(corr_flow['action_id']))
