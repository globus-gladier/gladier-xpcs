import os
import shortuuid

theta_conf = {'endpoint': '8f2f2eab-90d2-45ba-a771-b96e6d530cad',
              'local_endpoint': '8f2f2eab-90d2-45ba-a771-b96e6d530cad',
              'data_dir': '/projects/APSDataAnalysis/Braid/data/XPCS/',
              'proc_dir': '/projects/APSDataAnalysis/Braid/process/',
              'cont_dir': '/home/rvescovi/.funcx/containers/'}

conf = theta_conf    


#Set the name for the processing folder intermediate results
experiment_name = 'reprocess_eigen'
run_name = experiment_name + '_' + shortuuid.uuid()


flow_input = {
    "input": {
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
        "funcx_ep": conf['endpoint'],
        "funcx_local_ep": conf['local_endpoint'],
    }
}

