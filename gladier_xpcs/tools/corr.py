from gladier import GladierBaseTool, generate_flow_definition


def eigen_corr(event):
    import os
    import h5py
    import subprocess
    from subprocess import PIPE

    ##minimal data inputs payload
    imm_file = event['imm_file'] # raw data
    proc_dir = event['proc_dir'] # location of the HDF/QMAP process file / result
    hdf_file = event['hdf_file'] # name of the file to run EIGEN CORR

    ##optional
    corr_loc = event.get('corr_loc', 'corr')

    if not os.path.exists(proc_dir):
        raise NameError('proc dir does not exist')

    os.chdir(proc_dir)

    flags = ""
    with h5py.File(hdf_file, 'r') as data:
        try:
            df = data['measurement/instrument/acquisition/datafilename']
            dfn = str(df[()])
            if ".bin" in dfn:
                flags = "--rigaku"
            elif ".hdf" in dfn or ".h5" in dfn:
                flags = "--hdf5"
        except Exception as e:
            with open(os.path.join(proc_dir,'corr_error.log'), 'w+') as f:
                    f.write(str(e))

    cmd = f"{corr_loc} {hdf_file} -imm {imm_file} {flags}"
  
    res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE,
                             shell=True, executable='/bin/bash')
    
    with open(os.path.join(proc_dir,'corr_output.log'), 'w+') as f:
                f.write(res.stdout.decode('utf-8'))

    with open(os.path.join(proc_dir,'corr_errors.log'), 'w+') as f:
                f.write(res.stderr.decode('utf-8'))
    
    return str(res.stdout)

@generate_flow_definition(modifiers={
    eigen_corr: {'WaitTime': 600}
})
class EigenCorr(GladierBaseTool):

    required_input = [
        'proc_dir',
        'imm_file',
        'hdf_file',
        'flags',
        'flat_file',
        'corr_loc',
        'funcx_endpoint_compute',
    ]

    funcx_functions = [
        eigen_corr
    ]
