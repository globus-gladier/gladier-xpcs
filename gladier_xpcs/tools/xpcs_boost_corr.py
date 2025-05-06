from gladier import GladierBaseTool, generate_flow_definition

def xpcs_boost_corr(**data):
    import os
    import json
    import time
    import logging
    import subprocess
    import pathlib
    from boost_corr import __version__ as boost_version

    if not os.path.exists(data['proc_dir']):
        raise NameError(f'{data["proc_dir"]} \n Proc dir does not exist!')

    log_file = os.path.join(data['proc_dir'], 'boost_corr.log')

    os.chdir(data['proc_dir'])

    boost_corr = data['boost_corr']
    # usage: boost_corr [-h] -r RAW_FILENAME [-q QMAP_FILENAME] [-o OUTPUT_DIR] [-s SMOOTH] 
    # [-i GPU_ID] [-b BEGIN_FRAME] [-e END_FRAME] [-f STRIDE_FRAME]
    # [-a AVG_FRAME] [-t TYPE] [-d DQ_SELECTION] [-v] [-G] [-n] [-w] [-c CONFIG_JSON]

    cmd = [
        "boost_corr",
        "-r", boost_corr["raw"],
        "-q", boost_corr["qmap"],
        "-o", boost_corr["output"],
        "-i", str(boost_corr["gpu_id"]),
        "-s", boost_corr["smooth"],
        "-b", str(boost_corr["begin_frame"]),
        "-e", str(boost_corr["end_frame"]),
        "-f", str(boost_corr["stride_frame"]),
        "-a", str(boost_corr["avg_frame"]),
        "-t", boost_corr["type"],
        "-d", boost_corr["dq_selection"],
        "-G" if boost_corr["save_g2"] else "",
        "-w" if boost_corr["overwrite"] else "",
        "-v" if boost_corr["verbose"] else "",
    ]
    corr_start = time.time()
    result = subprocess.run([" ".join(cmd)], shell=True, capture_output=True, text=True)
    execution_time_seconds = round(time.time() - corr_start, 2)
    pathlib.Path(log_file).write_text(str(result.stderr))

    metadata = {
        'executable' : {
            'name': 'boost_corr',
            'tool_version': str(boost_version),
            'execution_time_seconds': execution_time_seconds,
            'device': 'gpu' if data['boost_corr'].get('gpu_flag', 0) >= 0 else 'cpu',
            'source': 'https://pypi.org/project/boost_corr/',
            }
    }

    if data.get('execution_metadata_file'):
        with open(data['execution_metadata_file'], 'w') as f:
            f.write(json.dumps(metadata, indent=2))
    logs = []
    if os.path.exists(log_file):
        with open(log_file) as f:
            logs = f.readlines()

    return {
        'result': 'SUCCESS',
        # 'boost_corr_log': logs,
        'proc_dir': data['proc_dir'],
        'boost_corr': data['boost_corr'],
        'execution_time_seconds': execution_time_seconds,
        'metadata': metadata,
    }


@generate_flow_definition(modifiers={
    xpcs_boost_corr: {'WaitTime': 7200,
                      'ExceptionOnActionFailure': True}
})
class BoostCorr(GladierBaseTool):

    required_input = [
        'proc_dir',
        'compute_endpoint',
    ]

    compute_functions = [
        xpcs_boost_corr
    ]


if __name__ == '__main__':
    data = {
        'proc_dir':'/eagle/APSDataAnalysis/nick/xpcs_gpu/C032_B315_A200_150C_att01_001_0001-1000',
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
    from pprint import pprint
    pprint(xpcs_boost_corr(**data))
