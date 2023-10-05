import logging
import globus_sdk

# from globus_portal_framework import load_globus_access_token
from django.conf import settings

log = logging.getLogger(__name__)


def batch_flow(reprocessing_info, run_inputs):
    sfc = _get_specific_flow_client(reprocessing_info["flow_id"], reprocessing_info["flow_scope"])
    run_permissions = [f"urn:globus:groups:id:{reprocessing_info['group']}"]

    for run_input in run_inputs:
        run = sfc.run_flow(
            body=run_input,
            label="TEST DGPF RUN",
            tags=["test", "run"],
            run_monitors=run_permissions,
            run_managers=run_permissions,
        )
    log.debug(f'Started {len(run_inputs)} runs.')



def _get_specific_flow_client(flow_id, scope):
    app = globus_sdk.ConfidentialAppAuthClient(settings.XPCS_FLOW_CLIENT, settings.XPCS_FLOW_SECRET)
    response = app.oauth2_client_credentials_tokens(requested_scopes=[scope])
    tokens = response.by_resource_server
    authorizer = globus_sdk.AccessTokenAuthorizer(tokens[flow_id]['access_token'])
    return globus_sdk.SpecificFlowClient(flow_id, authorizer=authorizer)
