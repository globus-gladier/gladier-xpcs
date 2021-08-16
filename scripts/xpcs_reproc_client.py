#!/usr/bin/env python 
"""
Convenience script to test XPCS reprocessing.
"""


# Enable Gladier Logging

import argparse
import os
from pprint import pprint

import gladier_xpcs.log
from gladier_xpcs.client_reprocess import XPCSReprocessingClient


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
                        default='/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('--imm', help='Path to the imm',
                        default='/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                                'A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--group', help='Visibility in Search', default=None)
    parser.add_argument('--source-globus-ep', help='Source Globus Endpoint (Default Petrel)',
                        default='e55b4eab-6d04-11e5-ba46-22000b92c6ec')
    parser.add_argument('--compute-globus-ep', help='Compute Globus Endpoint (Default Theta)',
                        default='08925f04-569f-11e7-bef8-22000b9a448b')
    parser.add_argument('--qmap-source-globus-ep', help='Source Qmap Globus Endpoint (Default Petrel)',
                        default='e55b4eab-6d04-11e5-ba46-22000b92c6ec')
    parser.add_argument('--qmap-source-globus-path', help='Source Qmap Globus Path',
                        default='/GlobusPortal_XPCS/sanat201903_qmap_S270_D54_lin.h5')
    parser.add_argument('--processing-dir', help='Location folder on compute endpoint to process data',
                        default='/projects/APSDataAnalysis/nick/gladier_testing/')

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
    qmap_file = os.path.join(dataset_dir, os.path.basename(args.qmap_source_globus_path))

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

            'qmap_source_endpoint': args.qmap_source_globus_ep,
            'qmap_source_path': args.qmap_source_globus_path,
            'qmap_destination_endpoint': args.compute_globus_ep,
            'qmap_file': qmap_file,
            'flat_file': 'not_used',

            'hdf_file_source': args.hdf,
            'imm_file_source': args.imm,
            'proc_dir': dataset_dir,
            'hdf_file': hdf_file,
            'imm_file': imm_file,
            # 'corr_loc': 'corr',
            'corr_loc': '/bin/echo',
            # 'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'flags': '',

            # globus endpoints
            'globus_endpoint_source': args.source_globus_ep,
            'globus_endpoint_proc': args.compute_globus_ep,

            # container hack for corr
            'eigen_corr_funcx_id': register_container()
        }
    }

    re_cli = XPCSReprocessingClient()
    pprint(re_cli.flow_definition)
    # re_cli.logout()
    pprint(re_cli.get_input())

    corr_flow = re_cli.run_flow(flow_input=flow_input)
    action_id = corr_flow['action_id']

    re_cli.progress(action_id)
    pprint(re_cli.get_status(action_id))