theta_conf = {'endpoint': '8f2f2eab-90d2-45ba-a771-b96e6d530cad',
              'local_endpoint': '8f2f2eab-90d2-45ba-a771-b96e6d530cad',
              'data_dir': '/projects/APSDataAnalysis/Braid/data/XPCS/',
              'proc_dir': '/projects/APSDataAnalysis/Braid/process/',
              'cont_dir': '/home/rvescovi/.funcx/containers/'}
              


#Set the name for the processing folder intermediate results
experiment_name = 'reprocess_eigen'
run_name = experiment_name + '_' + shortuuid.uuid()


flow_input = {
    "input": {
        #HTTPS-Download Container variables
        "container_server_url":"https://45a53408-c797-11e6-9c33-22000a1e3b52.e.globus.org/Braid/containers",
        "container_name": "eigen.simg",
        "container_path": conf['cont_dir'],
        "headers": headers,

        #HTTPS-Download Data variables
        "dataset_server_url": "https://45a53408-c797-11e6-9c33-22000a1e3b52.e.globus.org/Braid/data/xpcs_example/",
        "dataset_name": 'A001_Aerogel_qmap.tar.xz',
        "data_dir": conf['data_dir'],

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
        

        #Eigen funcX functions
        "corr_fxid": corr_fxid,
        "qmap_fxid": qmap_fxid,

        #Utility funcX functions
        "https_download_fxid": https_download_fxid,
        "unzip_data_fxid": unzip_data_fxid,

        # funcX endpoints 
        "funcx_ep": conf['endpoint'],
        "funcx_local_ep": conf['local_endpoint'],
    }
}

