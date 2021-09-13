#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import os
from time import sleep , strftime

import argparse
def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', default='samples_dm.txt')
    parser.add_argument('-n', default=10, type=int)
    parser.add_argument('-t', default=2, type=int)
    parser.add_argument('-l', default=None, type=str)
    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()
    test_num = 10
    dm_workflow = 'xpcs8-01-gladier'
    
    test_file = args.f
    samples = open(test_file, 'r').readlines()

    beamline_wait = args.t

    n_files = args.n

    if n_files == -1:
        n_files = len(samples)
    
    if not args.l:
        logfile = strftime("%Y%m%d_%H%M") + 'log.txt'

    print(f'Found {len(samples)} files') 
    print(f'Executing {n_files} files')
    for k in range(0,n_files):
        k_sample = samples[k]
        cmd = f'source /home/dm/etc/dm.setup.sh; dm-start-processing-job --workflow-name={dm_workflow} {k_sample} xpcsGroupName:/xpcs sgeQueueName:xpcs8new.q' 
        print(cmd)
        sleep(beamline_wait)
        os.system(cmd)
        # cmd get flow id? !!
