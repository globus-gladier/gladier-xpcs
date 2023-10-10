import logging
import globus_sdk

# from globus_portal_framework import load_globus_access_token
from django.conf import settings
from gladier_xpcs.deployments import deployment_map
from xpcs_portal.xpcs_index.apps import AVAILABLE_DEPLOYMENTS

log = logging.getLogger(__name__)

REPROCESSING_FLOW_DEPLOYMENTS = {
    d: {'deployment': deployment_map.get(d), 
    'authorization': settings.XPCS_DEPLOYMENTS[d]}
    for d in AVAILABLE_DEPLOYMENTS
    }


def batch_flow(reprocessing_flow, deployment, run_inputs):
    dep_info = REPROCESSING_FLOW_DEPLOYMENTS[deployment]
    sfc = get_authorized_flow(reprocessing_flow["flow_id"], reprocessing_flow["flow_scope"], dep_info["authorization"])
    run_permissions = [f"urn:globus:groups:id:{reprocessing_flow['group']}"]

    for run_input in run_inputs:
        run = sfc.run_flow(
            body=run_input,
            label=f"XPCS Run at {deployment}",
            tags=["reprocessing", deployment],
            run_monitors=run_permissions,
            run_managers=run_permissions,
        )
    log.debug(f'Started {len(run_inputs)} runs.')


def get_authorized_flow(flow_id, flow_scope, authorization):
    atype = authorization['type']
    log.info(f'Authorizing XPCS Reprocessing flow {flow_id} with type {atype}')
    if atype == 'app':
        app = globus_sdk.ConfidentialAppAuthClient(authorization['uuid'], authorization['client_secret'])
        response = app.oauth2_client_credentials_tokens(requested_scopes=[flow_scope])
        tokens = response.by_resource_server
        authorizer = globus_sdk.AccessTokenAuthorizer(tokens[flow_id]['access_token'])
    elif atype == 'token':
        client = globus_sdk.NativeAppAuthClient(authorization['client_id'])
        tokens = authorization['tokens'][flow_scope]
        # authorizer = globus_sdk.RefreshTokenAuthorizer(tokens['refresh_token'], client, access_token=tokens['access_token'], expires_at=tokens['expiration_time'])
        authorizer = globus_sdk.AccessTokenAuthorizer(tokens['access_token'])
    else:
        raise ValueError(f'Unable to authorize reprocessing flow {flow_scope}')
    return globus_sdk.SpecificFlowClient(flow_id, authorizer=authorizer)
