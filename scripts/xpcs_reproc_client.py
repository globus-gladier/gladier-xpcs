#!/usr/bin/env python 
"""
Convenience script to test XPCS reprocessing.
"""


# Enable Gladier Logging

import argparse
from pprint import pprint

import gladier_xpcs.log
from gladier_xpcs.flow_reprocess import XPCSReprocessingFlow
from gladier_xpcs.deployments import deployment_map


# This is a patch to continue using funcx 0.0.3 until the new AP comes online.
def register_container():
    from funcx.sdk.client import FuncXClient
    fxc = FuncXClient()
    from gladier_xpcs.tools.corr import eigen_corr
    cont_dir = '/eagle/APSDataAnalysis/XPCS_test/containers/'
    container_name = 'eigen_v2.simg'
    eigen_cont_id = fxc.register_container(location=cont_dir+container_name,container_type='singularity')
    corr_cont_fxid = fxc.register_function(eigen_corr, container_uuid=eigen_cont_id)
    return corr_cont_fxid


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--hdf', help='Path to the hdf file',
                        default='/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                                'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf')
    parser.add_argument('--imm', help='Path to the imm',
                        default='/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                                'A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm')
    parser.add_argument('--group', help='Visibility in Search', default=None)
    parser.add_argument('--qmap-source-globus-ep', help='Source Qmap Globus Endpoint (Default Petrel)',
                        default='e55b4eab-6d04-11e5-ba46-22000b92c6ec')
    parser.add_argument('--qmap-source-globus-path', help='Source Qmap Globus Path',
                        default='/GlobusPortal_XPCS/sanat201903_qmap_S270_D54_lin.h5')
    parser.add_argument('--deployment','-d', help=f'Deployment configs. Available: {list(deployment_map.keys())}',
                        required=True)
    return parser.parse_args()


if __name__ == '__main__':

    # Parse arguments
    args = arg_parse()
    depl = deployment_map.get(args.deployment)
    if not depl:
        raise ValueError(f'Invalid Deployment, deployments available: {list(deployment_map.keys())}')

    re_cli = XPCSReprocessingFlow()
    flow_input = re_cli.get_xpcs_input(depl, args.hdf, args.imm, args.qmap_source_globus_path)
    flow_input['input'].update({
        'qmap_source_endpoint': args.qmap_source_globus_ep,
        'qmap_source_path': args.qmap_source_globus_path,
        'eigin_corr_funcx_id': register_container(),
    })

    # print for context
    pprint(re_cli.flow_definition)
    pprint(flow_input)

    # Run flow, follow progress, print result
    corr_flow = re_cli.run_flow(flow_input=flow_input, label=re_cli.get_label(flow_input))
    action_id = corr_flow['action_id']
    re_cli.progress(action_id)
    pprint(re_cli.get_status(action_id))