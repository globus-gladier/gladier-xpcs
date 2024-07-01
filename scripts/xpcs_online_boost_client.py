#!/home/beams/8IDIUSER/.conda/envs/gladier/bin/python

## /home/beams/8IDIUSER/.conda/envs/gladier/bin/python /home/beams10/8IDIUSER/DM_Workflows/xpcs8/automate/raf/gladier-xpcs/scripts/xpcs_corr_client.py --hdf '/data/xpcs8/2019-1/comm201901/cluster_results/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf' --imm /data/xpcs8/2019-1/comm201901/A001_Aerogel_1mm_att6_Lq0_001/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm --group 0bbe98ef-de8f-11eb-9e93-3db9c47b68ba

import argparse
import os
import pathlib
import time

from gladier_xpcs.flows import XPCSBoost
from gladier_xpcs.deployments import deployment_map
from gladier_xpcs import log  # noqa Add INFO logging

from globus_sdk import ConfidentialAppAuthClient, AccessTokenAuthorizer
from gladier.managers.login_manager import CallbackLoginManager
from gladier import FlowsManager

from typing import List, Mapping, Union
import traceback
from fair_research_login import JSONTokenStorage

# Get client id/secret
CLIENT_ID = os.getenv("GLADIER_CLIENT_ID")
CLIENT_SECRET = os.getenv("GLADIER_CLIENT_SECRET")


def arg_parse():
    parser = argparse.ArgumentParser()

    parser.add_argument('--hdf', help='Path to the hdf (metadata) file',
                        default='/2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000/H001_27445_QZ_XPCS_test-01000.hdf')
    parser.add_argument('--raw', help='Path to the raw data file. Multiple formats (.imm, .bin, etc) supported',
                        default='/2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000/H001_27445_QZ_XPCS_test-01000.h5')
    parser.add_argument('--qmap', help='Path to the qmap file',
                        default='/2024-1/zhang202402_2/data/standard_qmaps/eiger4M_qmap_d36_s360.h5')
    parser.add_argument('--atype', default='Both', help='Analysis type to be performed. Available: Multitau, Twotime')
    parser.add_argument('--gpu_flag', type=int, default=0, help='''Choose which GPU to use. if the input is -1, then CPU is used''')
    # Group MUST not be None in order for PublishTransferSetPermission to succeed. Group MAY
    # be specified even if the flow owner does not have a role to set ACLs, in which case PublishTransferSetPermission will be skipped.
    parser.add_argument('--group', help='Visibility in Search', default='368beb47-c9c5-11e9-b455-0efb3ba9a670')
    parser.add_argument('--deployment','-d', default='aps8idi-polaris', help=f'Deployment configs. Available: {list(deployment_map.keys())}')
    parser.add_argument('--batch_size', default='256', help=f'Size of gpu corr processing batch')
    parser.add_argument('--verbose', default=False, action='store_true', help=f'Verbose output')

    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()

    deployment = deployment_map.get(args.deployment)
    if not deployment:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')
    elif deployment.service_account and not (os.getenv('GLADIER_CLIENT_ID') and os.getenv('GLADIER_CLIENT_SECRET')):
        raise ValueError(f'Deployment requires setting GLADIER_CLIENT_ID and GLADIER_CLIENT_SECRET')

    atype_options = ['Multitau', 'Both'] # "Twotime" is currently not supported!
    if args.atype not in atype_options:
        raise ValueError(f'Invalid --atype, must be one of: {", ".join(atype_options)}')

    fm = FlowsManager(flow_id="72e6469a-cf30-46bc-bff4-94dca46f2459", on_change=None)
    corr_flow = XPCSBoost(flows_manager=fm)

    flow_input = corr_flow.get_xpcs_input(
        deployment, args.raw, args.hdf, args.qmap,
        gpu_flag=args.gpu_flag, verbose=args.verbose, batch_size=args.batch_size, atype=args.atype, group=args.group,
    )

    corr_run_label = pathlib.Path(args.hdf).name[:62]
    flow_run = corr_flow.run_flow(flow_input=flow_input, label=corr_run_label, tags=['aps', 'xpcs'])

    print('run_id : ' + flow_run['action_id'])