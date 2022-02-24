from gladier import GladierBaseTool, generate_flow_definition


def eigen_corr_gpu(**event):
    import os
    import subprocess
    from subprocess import PIPE

    ##minimal data inputs payload
    proc_dir = event.get('proc_dir') # location of transfered files / results
    raw_file = event.get('raw_file') # raw data
    qmap_file = event.get('qmap_file') # name of the qmap file
    corr_gpu_loc = event.get('corr_gpu_loc') #location of processing script
    corr_gpu_id = event.get('corr_gpu_id', 0) # Use GPU or CPU? -1 for cpu, 0 for gpu
    batch_size = event.get('batch_size') #processing batch size
    verbose = event.get('verbose')

    if not os.path.exists(proc_dir):
        raise NameError(f'{proc_dir} \n Proc dir does not exist!')

    os.chdir(proc_dir)

    #gpu corr need a batchinfo file to exist in the input directory but does not use contents
    with open(f'{proc_dir}/input/scratch.batchinfo', 'w+') as f:
        f.write('This file can be ignored.')

    cmd = f"python {corr_gpu_loc} --gpu_id {corr_gpu_id}  --batch_size {batch_size} -q {qmap_file} -r {raw_file} --output {proc_dir}/output"
    if verbose:
        cmd += " --verbose"
    res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE,
                             shell=True, executable='/bin/bash')

    with open(os.path.join(proc_dir,'corr_output.log'), 'w+') as f:
                f.write(res.stdout.decode('utf-8'))

    with open(os.path.join(proc_dir,'corr_errors.log'), 'w+') as f:
                f.write(res.stderr.decode('utf-8'))

    return str(res.stdout)


@generate_flow_definition(modifiers={
    eigen_corr_gpu: {'WaitTime': 7200}
})
class EigenCorrGPU(GladierBaseTool):

    required_input = [
        'proc_dir',
        'raw_file',
        'qmap_file',
        'verbose',
        'batch_size',
        'corr_gpu_loc',
        'funcx_endpoint_compute',
    ]

    funcx_functions = [
        eigen_corr_gpu
    ]
