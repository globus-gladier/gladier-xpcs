from gladier import GladierBaseTool, generate_flow_definition

def gpu_multitau(**data):
    import os
    import json
    import traceback
    from boost_corr.xpcs_aps_8idi.gpu_corr_multitau import solve_multitau

    ##minimal data inputs payload
    proc_dir = data.get('proc_dir') # location of transfered files / results
    raw_file = data.get('raw_file') # raw data
    qmap_file = data.get('qmap_file') # name of the qmap file
    config_file = data.get('config_file', None) # name of the config file
    gpu_flag = data.get('gpu',0)
    verbose = data.get('verbose', False)

    if not os.path.exists(proc_dir):
        raise NameError(f'{proc_dir} \n Proc dir does not exist!')

    os.chdir(proc_dir)

    # if config_file:
    #     with open(config_file) as f:
    #         config = json.load(f)

    ans = None
    try:
        ans = solve_multitau(qmap=qmap_file,
                raw=raw_file,
                output=proc_dir,
                batch_size=8,
                gpu_id=gpu_flag,
                verbose=verbose,
                masked_ratio_threshold=0.75,
                use_loader=True,
                begin_frame=3,
                end_frame=-1,
                avg_frame=7,
                stride_frame=5,
                overwrite=False)
    except Exception:
        flag = 1
        traceback.print_exc()

    return(ans)


@generate_flow_definition(modifiers={
    gpu_multitau: {'WaitTime': 7200}
})
class MultitauGPU(GladierBaseTool):

    required_input = [
        'proc_dir',
        'raw_file',
        'qmap_file',
        'funcx_endpoint_compute',
    ]

    funcx_functions = [
        gpu_multitau
    ]
