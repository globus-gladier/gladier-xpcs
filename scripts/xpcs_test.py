#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

import subprocess

test_num = 10
dm_workflow = 'xpcs8-01-gladier'
test_file = 'xpcs_test.txt'

beamline_wait = 1 

samples = open(test_file, 'r')

for k_sample in samples:
    cmd = 'source /home/dm/etc/dm.setup.sh; dm-start-processing-job --workflow-name=' + dm_workflow + ' ' + k_sample 
    print(cmd)
#    result = subprocess.run(cmd, stdout=subprocess.PIPE)
