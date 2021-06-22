#!/home/beams/8IDIUSER/.conda/envs/automate/bin/python
# -*- coding: utf-8 -*-

"""
Automate XPCS: Initiate processing of an XPCS file.
"""

import argparse
import time
import json
import sys
import os
import sys
import pprint

if __name__ == '__main__':
    # Allow running via command line
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from XPCS.tools.client import XPCSClient
from XPCS.triggers.crawl_clutch import search_dir, get_datasets



def main(arg):
    """
    Process an input file via an automate flow. To initiate the flow we need a flow_id and flow_scope.

    Version 3 (06/9/19): This assumes we take data from an endpoint (clutch/petrel), move to alcf, process it,
    then move it back to the origional endpoint in the path $inputDir/automate/$inputFileName.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="Data to process.",
                        default='/data/xpcs8/2019-1/comm201901/cluster_results/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument("--endpoint", help="Source endpoint.", default='fdc7e74a-fa78-11e8-9342-0e3d676669f4')
    parser.add_argument("--imm", help="Path to the imm on the endpoint.",
                        default='/data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001'
                                '/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--expression', '-e', help='Use Regex to determine datasets to run. '
                                                   'EX: /data/xpcs8/2020-1/foster202003/A38[0-5]_PIB_')
    parser.add_argument("--dry_run", help="If set the flow is not started.",
                        action='store_true')
    parser.add_argument("--block", help="If set, the script will wait until the flow completes.",
                        action='store_true')
    parser.add_argument("--rigaku", help="If set, the corr script use the --rigaku flag.",
                        action='store_true')
    args = parser.parse_args()

    xpcs = XPCSClient()

    if args.expression:
        base, exp = os.path.dirname(args.expression), os.path.basename(args.expression)
        print(f'Searching "{base}" with pattern "{exp}"')
        candidates = search_dir(base, pattern=exp)
        print('\n'.join(candidates))
        print(f'\n{len(candidates)} candidates found, generating valid list...')

        datasets = get_datasets([os.path.join(base, c) for c in candidates])
        print(f'INVALID DATASETS:')
        pprint.pprint(datasets['invalid'])
        print(f'VALID DATASETS:')
        pprint.pprint(datasets['valid'])
        # They do not contain a hdf and imm file.
        print(f'({len(datasets["valid"])}/{len(candidates)}) datasets are valid. '
              f'{len(datasets["invalid"])} datasets are invalid.')
        pathnames = datasets['valid_datasets']
    else:
        pathnames = [(args.input, args.imm)]

    flow_options = None
    if args.rigaku:
        exec_kwargs = {'rigaku': True}
        flow_options = {'exec': exec_kwargs}

    flow_input = xpcs.create_flow_input(pathnames=pathnames, flow_options=flow_options)

    if args.dry_run:
        print(json.dumps(flow_input, indent=2))
        sys.exit()

    #if input(f'Start Automate flow with {len(pathnames)} datasets? Y/N>') in ['y', 'Y']:
    flow_id = xpcs.start_flow(flow_input)
    pprint.pprint(flow_id)
    #else:
    #    print('Aborting due to user input...')


if __name__ == "__main__":
    main(sys.argv[1:])
