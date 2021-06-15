#!/usr/bin/env python 

# Import Gladier base
from gladier import GladierBaseClient, generate_flow_definition
# Enable Gladier Logging
import gladier.tests

from pprint import pprint

@generate_flow_definition
class XPCS_Client(GladierBaseClient):
    gladier_tools = [
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
    eigen_corr_cont_id = fxc.register_function(eigen_corr, container_uuid=eigen_cont_id)
    return eigen_corr_cont_id
    ##

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Data to process.",
                    default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                            'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument("--imm", help="Path to the imm on the endpoint.",
                    default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                            '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument("--endpoint", help="Source endpoint.", default='fdc7e74a-fa78-11e8-9342-0e3d676669f4')
    # parser.add_argument('--expression', '-e', help='Use Regex to determine datasets to run. '
    #                                             'EX: /data/xpcs8/2020-1/foster202003/A38[0-5]_PIB_')
    # parser.add_argument("--dry_run", help="If set the flow is not started.",
    #                 action='store_true')
    # parser.add_argument("--block", help="If set, the script will wait until the flow completes.",
    #                 action='store_true')
    # parser.add_argument("--rigaku", help="If set, the corr script use the --rigaku flag.",
    #                 action='store_true')
    args = parser.parse_args()

def create_payload(self, hdf_pathname, imm_pathname, options=None):

    options = options 
    hdf_name = os.path.basename(hdf_pathname)
    imm_name = os.path.basename(imm_pathname)


    input_parent_dir = hdf_name.replace('.hdf', '')

    # Generate Destination Pathnames.
    base_proc = os.path.join(options['transfer']['proc_path'], input_parent_dir)
    proc_hdf_file = os.path.join(base_proc, hdf_name)
    proc_imm_file = os.path.join(base_proc, imm_name)
    result_dirname = f"{input_parent_dir}/ALCF_results/{hdf_name}"
    info = {
        'hdf_filename': hdf_name,
        'imm_filename': imm_name,
        'source_hdf_abspath': hdf_pathname,
        'source_imm_abspath': imm_pathname,
        'proc_hdf_abspath': proc_hdf_file,
        'proc_imm_abspath': proc_imm_file,
        'proc_dir_abspath': os.path.dirname(proc_hdf_file),
        'proc_dirname': input_parent_dir,
        'result_abspath': result_dirname,
    }

    exec_options = options.get('exec', {})
    metadata = options.get('metadata', {})
    metadata['reprocessing'] = {
        'source_endpoint': options['transfer']['aps_ep'],
        'source_hdf_abspath': hdf_pathname,
        'source_imm_abspath': imm_pathname,
    }
    funcx_payload = {
        'data': {
            'hdf': proc_hdf_file,
            'imm': proc_imm_file,
            'metadata': metadata,
            'metadata_file': proc_hdf_file.replace(".hdf", ".json"),
        }
    }
    if 'rigaku' in exec_options:
        funcx_payload['data']['flags'] = "--rigaku"

    return {'info': info, 'payload': funcx_payload}

if __name__ == '__main__':

    args = parse_args()
    

    ##Process endpoints - theta
    local_endpoint = '8f2f2eab-90d2-45ba-a771-b96e6d530cad'
    queue_endpoint = '23519765-ef2e-4df2-b125-e99de9154611'
    ##Transfer endpoints - 
    beamline_ep='87c4f45e-9c8b-11eb-8a8c-d70d98a40c8d'
    theta_ep='08925f04-569f-11e7-bef8-22000b9a448b'

    ##stills_cont_fxid = register_container() ##phase out with containers

    base_input = {
        "input": {
            #Processing variables
            "base_local_dir": local_dir,
            "base_data_dir": data_dir,

            #Processing variables
            "proc_dir": os.path.join(conf['proc_dir'],run_name),

            #Eigen specific variables
            "hdf": "A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf",
            "imm": "A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm",
            "flags": '',

            # QMap data
            "qmap": "sanat201903_qmap_S270_D54_lin.h5",
            "flatfield": "Flatfiel_AsKa_Th5p5keV.hdf",
            "output": "A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf".replace('.hdf', '_qmap.hdf'),

            # funcX endpoints
            "funcx_local_ep": local_endpoint,
            "funcx_queue_ep": queue_endpoint,

            # globus endpoints
            "globus_local_ep": beamline_ep,
            "globus_dest_ep": theta_ep, 

            # container hack for stills
            "stills_cont_fxid": stills_cont_fxid
        }
    }

    payload = create_payload(pathnames=pathnames, flow_options=flow_options)


    corr_cli = XPCS_Client()
    corr_flow = corr_cli.run_flow(flow_input=payload)
    corr_cli.check_flow(corr_flow['action_id'])

