#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import subprocess
from time import sleep

import argparse
def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', default='xpcs_test.txt')
    parser.add_argument('--n', default=50)
    parser.add_argument('--t', default=5)
    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()
    test_num = 10
    dm_workflow = 'xpcs8-01-gladier'
    
    test_file = args.f
    samples = open(test_file, 'r')

    beamline_wait = args.t

    n_files = args.n

    for k_sample in samples:
        cmd = 'source /home/dm/etc/dm.setup.sh; dm-start-processing-job --workflow-name=' + dm_workflow + ' ' + k_sample 
        print(cmd)
        sleep(beamline_wait)
    #    result = subprocess.run(cmd, stdout=subprocess.PIPE)

