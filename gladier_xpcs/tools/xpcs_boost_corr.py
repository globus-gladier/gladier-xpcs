from gladier import GladierBaseTool, generate_flow_definition

def xpcs_boost_corr(boost_corr: dict, **data):
    import os
    import json
    import time
    import logging
    import subprocess
    import pathlib
    from boost_corr import __version__ as boost_version

    # Ensure the output path exists for the corr results file
    pathlib.Path(boost_corr['output']).mkdir(parents=True, exist_ok=True)
    # Set the CWD for Corr. There is no reason for this, it's simply defensive
    # in case corr outputs any file we don't expect.
    os.chdir(boost_corr['output'])

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

    log_file = pathlib.Path(boost_corr["output"]) / "boost_corr.log"
    log_file.write_text(str(result.stderr))

    metadata = {
        'tools' : [{
            'name': 'boost_corr',
            'tool_version': str(boost_version),
            'execution_time_seconds': execution_time_seconds,
            'device': 'gpu' if boost_corr.get('gpu_flag', 0) >= 0 else 'cpu',
            'source': 'https://pypi.org/project/boost_corr/',
            }]
    }

    if result.returncode != 0:
        raise Exception(str(result.stderr))

    return {
        'result': 'SUCCESS',
        'boost_corr': boost_corr,
        'execution_time_seconds': execution_time_seconds,
        'metadata': metadata,
    }


@generate_flow_definition(modifiers={
    xpcs_boost_corr: {'WaitTime': 604800,
                      'ExceptionOnActionFailure': True}
})
class BoostCorr(GladierBaseTool):

    required_input = [
        'boost_corr',
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
