#!/usr/bin/env python 

# Enable Gladier Logging
import gladier.tests

import argparse
import os
from pprint import pprint

from gladier_xpcs.client_reprocess import XPCSReprocessing


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
    parser.add_argument('--manifest_id', help='Visibility in Search', 
                        default='55dc53cf-593a-40d0-aab6-fcea1c1d05a4')
    parser.add_argument('--manifest_path', help='Visibility in Search', 
                        default='globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/nick/gladier_testing/'),
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

    manifest_id = args.manifest_id
    manifest_path = args.manifest_path


    flow_input = {
        'input': {
            # Manifest input files, and destination for where to process (Anyone can use the manifest below)
            'manifest_id': manifest_id,
            'manifest_destination': manifest_path,

            # Qmap inputs
            'qmap_source_endpoint': 'e55b4eab-6d04-11e5-ba46-22000b92c6ec',
            'qmap_source_path': '/XPCSDATA/Automate/qmap/sanat201903_qmap_S270_D54_lin.h5',
            'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
            'qmap_file': '/projects/APSDataAnalysis/nick/gladier_testing/sanat201903_qmap_S270_D54_lin.h5',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',

            # Pilot inputs (Renames the dataset)
            'reprocessing_suffix': '_nick_reprocessing_test',
            

            # Useful for turning off "manifest_to_list" and using a custom payload
            'parameter_file': '/projects/APSDataAnalysis/nick/gladier_testing/A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/parameters.json',

            'proc_dir': dataset_dir,
            'hdf_file': hdf_file,
            'imm_file': imm_file,
            'corr_loc': 'corr',
            'flags': '',

            # funcX endpoints
            # Should think of moving those to a cfg with better naming
            'funcx_endpoint_non_compute':'e449e8b8-e114-4659-99af-a7de06feb847',
            'funcx_endpoint_compute':    '4c676cea-8382-4d5d-bc63-d6342bdb00ca',


            # globus endpoints
            'globus_endpoint_clutch': args.source_globus_ep,
            'globus_endpoint_theta': args.compute_globus_ep,

            # container hack for corr 
            'eigen_corr_funcx_id': register_container()

        }
    }

    re_cli = XPCSReprocessing()
    pprint(re_cli.flow_definition)
    # re_cli.logout()
    pprint(re_cli.get_input())

    corr_flow = re_cli.run_flow(flow_input=flow_input)
    action_id = corr_flow['action_id']

    re_cli.progress(action_id)
    pprint(re_cli.get_status(action_id))