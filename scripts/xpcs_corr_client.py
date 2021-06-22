#!/usr/bin/env python 

# Import Gladier base
from gladier import GladierBaseClient, generate_flow_definition
# Enable Gladier Logging
import gladier.tests

import argparse
import os


@generate_flow_definition()
class XPCS_Client(GladierBaseClient):
    gladier_tools = [
        'gladier_xpcs.tools.transfer_qmap.TransferQmap',
        'gladier_xpcs.tools.EigenCorr',
        'gladier_xpcs.tools.ApplyQmap',
        'gladier_xpcs.tools.reprocessing.plot.MakeCorrPlots',
        'gladier_xpcs.tools.reprocessing.custom_pilot.CustomPilot',
    ]

##This is a patch to continue using funcx 0.0.3 until the new AP comes online.
def register_container():
    ##hacking over container for XPCS
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_xpcs.tools.corr import eigen_corr
    ##Move this to an XPCS common Folder
    cont_dir =  '/home/rvescovi/.funcx/containers/'
    container_name = "eigen_v1.simg"
    eigen_cont_id = fxc.register_container(location=cont_dir+'/'+container_name,container_type='singularity')
    corr_cont_fxid = fxc.register_function(eigen_corr, container_uuid=eigen_cont_id)
    return corr_cont_fxid
    ##

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hdf", help="Path to the hdf file",default='')
    parser.add_argument("--imm", help="Path to the imm", default='')
    parser.add_argument("--group", help="Globus group for pilot", default=None)
    parser.add_argument("--endpoint", help="Source endpoint", default=None)
    args = parser.parse_args()

def create_payload(base_input, args):

    hdf_pathname = args.get('hdf','')
    imm_pathname = args.get('imm','')

    hdf_name = os.path.basename(hdf_pathname)
    imm_name = os.path.basename(imm_pathname)

    input_parent_dir = hdf_name.replace('.hdf', '')

    # Generate Destination Pathnames.
    base_proc = os.path.join(options['transfer']['proc_path'], input_parent_dir)
    proc_hdf_file = os.path.join(base_proc, hdf_name)
    proc_imm_file = os.path.join(base_proc, imm_name)
    result_dirname = f"{input_parent_dir}/ALCF_results/{hdf_name}"

    # info = {
    #     'hdf_filename': hdf_name,
    #     'imm_filename': imm_name,
    #     'source_hdf_abspath': hdf_pathname,
    #     'source_imm_abspath': imm_pathname,
    #     'proc_hdf_abspath': proc_hdf_file,
    #     'proc_imm_abspath': proc_imm_file,
    #     'proc_dir_abspath': os.path.dirname(proc_hdf_file),
    #     'proc_dirname': input_parent_dir,
    #     'result_abspath': result_dirname,
    # }


    base_input['input'] = {
            'hdf': proc_hdf_file,
            'imm': proc_imm_file,
            'metadata': metadata,
            'metadata_file': proc_hdf_file.replace(".hdf", ".json"),
        }

    return base_input

if __name__ == '__main__':

    args = arg_parse()
    
    ##Process endpoints
    theta_non_compute_ep = '8f2f2eab-90d2-45ba-a771-b96e6d530cad'
    theta_compute_ep = '23519765-ef2e-4df2-b125-e99de9154611'
    ##Transfer endpoints
    beamline_transfer_ep='87c4f45e-9c8b-11eb-8a8c-d70d98a40c8d'
    eagle_transfer_ep='05d2c76a-e867-4f67-aa57-76edeb0beda0'

    corr_cont_fxid = register_container() ##phase out with containers

    base_input = {
        "input": {
            # funcX endpoints
            "funcx_endpoint_non_compute": theta_non_compute_ep,
            "funcx_endpoint_compute": theta_compute_ep,

            # globus endpoints
            "globus_local_ep": beamline_ep,
            "globus_dest_ep": theta_ep, 

            # container hack for corr 
            "eigen_corr_fxid": corr_cont_fxid
        }
    }

    payload = create_payload(base_input, args)


    corr_cli = XPCS_Client()
    corr_flow = corr_cli.run_flow(flow_input=payload)
    corr_cli.check_flow(corr_flow['action_id'])

