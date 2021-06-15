from gladier.client import GladierClient
from payload import * 



from gladier_xpcs.flows.eigen_flow import corr_basic_flow_definition

class XPCS_Client(GladierClient):
    gladier_tools = [
        'gladier_tools.xpcs.EigenCorr',
        'gladier_tools.xpcs.ApplyQmap',
    ]
    flow_definition = corr_basic_flow_definition

def XPCSLogic(event_file):

    new_file = os.path.basename(event_file)

    cbf_pattern = r'(\w+_\d+_)(\d+).cbf'
    cbf_parse = re.match(cbf_pattern, new_file)

    ##check if extra_args
    extra_folder = event_file.replace(local_dir,'')
    extra_folder = extra_folder.replace(new_file,'')
    extra_folder = extra_folder.replace('/','')

    ##processing dirs
    data_dir = os.path.join(base_input["input"]["base_data_dir"], extra_folder)
    base_input["input"]["local_dir"] = os.path.join(base_input["input"]["base_local_dir"], extra_folder)
    base_input["input"]["data_dir"] = data_dir
    base_input["input"]["proc_dir"] = data_dir + '_proc'
    base_input["input"]["upload_dir"] = data_dir + '_images' 

    if cbf_parse is not None:
        cbf_base =cbf_parse.group(1)
        cbf_num =int(cbf_parse.group(2))

        n_batch_transfer = 64
        n_batch_plot = 1024
        
        range_delta = n_batch_transfer

        if cbf_num%n_batch_transfer==0:
            subranges = create_ranges(cbf_num-n_batch_transfer, cbf_num, range_delta)
            new_range = subranges[0]
            print('Transfer+Stills Flow')
            base_input["input"]["input_files"]=f"{cbf_base}{new_range}.cbf"
            base_input["input"]["input_range"]=new_range[1:-1]
            base_input["input"]["trigger_name"]= os.path.join(
                base_input["input"]["data_dir"], new_file
            )
            flow_input = base_input
            #print("  Range : " + base_input["input"]["input_range"])
            #print(flow_input)
            workshop_flow = kanzus_workshop_client.start_flow(flow_input=flow_input)
            print("  Trigger : " + base_input["input"]["trigger_name"])
            print("  Range : " + base_input["input"]["input_range"])
            print("  UUID : " + workshop_flow['action_id'])
            print('')

        if cbf_num%n_batch_plot==0:
            print('Plot Flow')
            print("  UUID : " + workshop_flow['action_id'])
            print('')

def register_container():
    ##hacking over container
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_kanzus.tools.dials_stills import funcx_stills_process as stills_cont
    cont_dir =  '/home/rvescovi/.funcx/containers/'
    container_name = "dials_v1.simg"
    dials_cont_id = fxc.register_container(location=cont_dir+'/'+container_name,container_type='singularity')
    stills_cont_fxid = fxc.register_function(stills_cont, container_uuid=dials_cont_id)
    return stills_cont_fxid
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

def create_funcx_payload(self, hdf_pathname, imm_pathname, options=None):

    options = options or self.FLOW_DEFAULTS
    # Get source pathnames
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

    flow_input = xpcs.create_flow_input(pathnames=pathnames, flow_options=flow_options)



    corr_cli = XPCS_Client()
    corr_flow = corr_cli.run_flow(flow_input=data)
    corr_cli.check_flow(corr_flow['action_id'])




    

    



    kanzus_workshop_client = KanzusSSXGladier()

    exp = KanzusTriggers(local_dir)
    exp.run()

