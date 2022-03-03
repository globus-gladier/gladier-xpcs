from gladier import GladierBaseTool, generate_flow_definition

def xpcs_boost_corr(**data):
    import os
    import json
    from boost_corr.xpcs_aps_8idi import gpu_corr_multitau, gpu_corr_twotime
    from boost_corr import __version__ as boost_version

    if not os.path.exists(data['proc_dir']):
        raise NameError(f'{data["proc_dir"]} \n Proc dir does not exist!')

    os.chdir(data['proc_dir'])

    atype = data['boost_corr'].pop('atype')


    if atype in ('Multitau', 'Both'):
        gpu_corr_multitau.solve_multitau(**data['boost_corr'])
    elif atype in ('Twotime', 'Both'):
        gpu_corr_twotime.solve_twotime(**data['boost_corr'])

    
    metadata = {
        'executable' : {
            'name': 'boost_corr',
            'tool_version': str(boost_version),
            'device': 'gpu' if data['boost_corr'].get('gpu_flag', 0) >= 0 else 'cpu',
            'source': 'https://pypi.org/project/boost_corr/',
            }
    }

    if data.get('execution_metadata_file'):
        with open(data['execution_metadata_file'], 'w') as f:
            f.write(json.dumps(metadata, indent=2))

    return {
        'result': 'SUCCESS',
        'proc_dir': data['proc_dir'],
        'boost_corr': data['boost_corr'],
    }


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
