from gladier import GladierBaseTool, generate_flow_definition

def xpcs_boost_corr(**data):
    import os
    import json
    from boost_corr.xpcs_aps_8idi import gpu_corr_multitau, gpu_corr_twotime

    if not os.path.exists(data['proc_dir']):
        raise NameError(f'{data["proc_dir"]} \n Proc dir does not exist!')

    os.chdir(data['proc_dir'])

    atype = data['boost_corr'].pop('atype')


    if atype in ('Multitau', 'Both'):
        gpu_corr_multitau.solve_multitau(**data['boost_corr'])
    elif atype in ('Twotime', 'Both'):
        gpu_corr_twotime.solve_twotime(**data['boost_corr'])

    return_values = {}
    # These are assumed by the boost_corr client. Look for them ad append them to output
    errors = os.path.join(data['proc_dir'], 'corr_errors.log')
    output = os.path.join(data['proc_dir'], 'corr_output.log')
    if os.path.exists(errors):
        with open(errors) as f:
            return_values['errors'] = f.read()
    if os.path.exists(output):
            return_values['output'] = f.read()

    return return_values


@generate_flow_definition(modifiers={
    xpcs_boost_corr: {'WaitTime': 7200}
})
class BoostCorr(GladierBaseTool):

    required_input = [
        'proc_dir',
        'funcx_endpoint_compute',
    ]

    funcx_functions = [
        xpcs_boost_corr
    ]


if __name__ == '__main__':
    data = {
        'proc_dir':'/eagle/APSDataAnalysis/nick/xpcs_gpu',
        'boost_corr': {
            'raw':'C032_B315_A200_150C_att01_001_0001-1000/input/C032_B315_A200_150C_att01_001_00001-01000.imm',
            'qmap':'C032_B315_A200_150C_att01_001_0001-1000/qmap/bates202202_qmap_Lq1_ccdz25_S270_D54.h5',
            'verbose':True,
            'atype': 'Both',

            'output': 'C032_B315_A200_150C_att01_001_0001-1000/qmap/',
            'batch_size': 8,
            'gpu_id': 0,
            'verbose': True,
            'masked_ratio_threshold': 0.75,
            'use_loader': True,
            'begin_frame': 1,
            'end_frame': -1,
            'avg_frame': 1,
            'stride_frame': 1,
            'overwrite': False
        }
    }
    
    xpcs_boost_corr(**data)
