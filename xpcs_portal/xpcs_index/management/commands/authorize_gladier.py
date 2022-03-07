from pprint import pprint
import os
import sys
from gladier_xpcs.flows.flow_reprocess import XPCSReprocessingFlow
from django.core.management.base import BaseCommand
from automate_app.models import FlowInstanceAuthorizer, Flow
import globus_sdk
from django.contrib.auth.models import User
from django.utils.timezone import now


class Command(BaseCommand):
    help = 'Registers a FuncX Function for an Automate Flow'

    def add_arguments(self, parser):
        parser.add_argument('--user', required=False)

    def handle(self, *args, **options):
        gcli = XPCSReprocessingFlow()
        gcli.login()
        # Check register a new flow
        gcli.register_flow()
        # Check register new funcx function ids
        funcx_functions = gcli.get_funcx_function_ids()
        # Login with the new flow id, if one was deployed
        scopes = gcli.scopes + ['openid', 'profile', 'email']
        nc = gcli.get_native_client()
        nc.login(requested_scopes=scopes, refresh_tokens=True)
        tbs = gcli.get_native_client().load_tokens_by_scope()
        gconfig = gcli.get_cfg()[gcli.section]
        flow_info = tbs[gconfig['flow_scope']]
        assert gconfig['flow_id'], f'Check Flow ID!'

        auth_cli = globus_sdk.AuthClient(authorizer=nc.get_authorizers()['auth.globus.org'])
        user_info = auth_cli.oauth2_userinfo()

        try:
            print(f'Checking {gconfig["flow_id"]}')
            existing_flow = Flow.objects.get(flow_id=gconfig['flow_id'])
            if existing_flow.definition_checksum != gconfig['flow_checksum']:
                print(f'Updating {existing_flow}, checksum has changed!')
                existing_flow.definition_checksum = gconfig['flow_checksum']
                existing_flow.date_updated = now()
            print(f'Flow authorized: {existing_flow}')
            sys.exit(0)
        except Flow.MultipleObjectsReturned:
            flows = list(Flow.objects.filter(flow_id=gconfig['flow_id']))
            for f in flows[1:]:
                f.delete()
            print(f'Deleted extra flows, please run this again...')
            sys.exit(0)
        except Flow.DoesNotExist:
            print(f'Flow {gcli.section} does not exist, registering...')

        user = self.get_user(user_info['preferred_username'])
        if not flow_info.get('refresh_token'):
            print(f'Registration FAILED, no refresh token, please re-run.')
            nc.logout()
            sys.exit(1)
        flow = Flow(
            flow_id=gconfig['flow_id'],
            title=gcli.section,
            scope=gconfig['flow_scope'],
            definition_checksum=gconfig['flow_checksum'],
        )
        flow.save()
        fia = FlowInstanceAuthorizer(user=user,
                                     flow=flow,
                                     client_id=gcli.client_id,
                                     access_token=flow_info['access_token'],
                                     refresh_token=flow_info['refresh_token'],
                                     expires_at_seconds=float(flow_info['expires_at_seconds']))
        fia.save()
        print(f'You should save these funcx ids to a deployment')
        from pprint import pprint
        pprint(funcx_functions)

    def get_user(self, username):
        try:
            return User.objects.get(username=username)
        except User.DoesNotExist:
            users = [u.username for u in User.objects.all()]
            print(f'No user "{username}" found, please choose from '
                  f'the following: {users}')
            sys.exit()
