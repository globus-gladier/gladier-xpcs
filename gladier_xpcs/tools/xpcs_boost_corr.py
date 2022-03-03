from gladier import GladierBaseTool, generate_flow_definition

def xpcs_boost_corr(**data):
    import os
    import json

    ##minimal data inputs payload
    proc_dir = data.get('proc_dir') # location of transfered files / results
    raw_file = data.get('raw_file') # raw data
    qmap_file = data.get('qmap_file') # name of the qmap file
    atype = data.get('atype','Both') #["Multitau", "Twotime", "Both"]
    config_file = data.get('config_file', None) # name of the config file
    gpu_flag = data.get('gpu_flag',0)
    verbose = data.get('verbose', False)

    if not os.path.exists(proc_dir):
        raise NameError(f'{proc_dir} \n Proc dir does not exist!')

    os.chdir(proc_dir)

    # if config_file:
    #     with open(config_file) as f:
    #         config = json.load(f)

    if atype == 'Multitau' or atype == 'Both':
        from boost_corr.xpcs_aps_8idi.gpu_corr_multitau import solve_multitau
        solve_multitau(qmap=qmap_file,
                    raw=raw_file,
                    output=proc_dir,
                    batch_size=8,
                    gpu_id=gpu_flag,
                    verbose=verbose,
                    masked_ratio_threshold=0.75,
                    use_loader=True,
                    begin_frame=1,
                    end_frame=-1,
                    avg_frame=1,
                    stride_frame=1,
                    overwrite=False)

    if atype == 'Twotime' or atype == 'Both':
        from boost_corr.xpcs_aps_8idi.gpu_corr_twotime import solve_twotime
        solve_twotime(qmap=qmap_file,
                    raw=raw_file,
                    output=proc_dir,
                    batch_size=256,
                    gpu_id=gpu_flag,
                    verbose=verbose,
                    begin_frame=1,
                    end_frame=-1,
                    avg_frame=1,
                    stride_frame=1,
                    dq_selection=None,
                    smooth='sqmap')

    return


@generate_flow_definition(modifiers={
    xpcs_boost_corr: {'WaitTime': 7200}
})
class BoostCorr(GladierBaseTool):

    required_input = [
        'proc_dir',
        'raw_file',
        'qmap_file',
        'funcx_endpoint_compute',
    ]

    funcx_functions = [
        xpcs_boost_corr
    ]


if __name__ == '__main__':
    data = {'proc_dir':'/eagle/APSDataAnalysis/raf/xpcs_gpu',
    'raw_file':'C032_B315_A200_150C_att01_001_0001-1000/input/C032_B315_A200_150C_att01_001_00001-01000.imm',
    'qmap_file':'C032_B315_A200_150C_att01_001_0001-1000/qmap/bates202202_qmap_Lq1_ccdz25_S270_D54.h5',
    'verbose':True}
    
    xpcs_boost_corr(**data)

