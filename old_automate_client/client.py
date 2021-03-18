import logging
import os

from XPCS.tools.funcx_functions import xpcs_corr, xpcs_plot, xpcs_pilot
from XPCS.tools.automate_flows import flow_definition

from globus_automate_client import (create_flows_client, graphviz_format, state_colors_for_log,
                                    get_access_token_for_scope, create_action_client,
                                    create_flows_client)

from configobj import ConfigObj

import funcx.sdk.client

log = logging.getLogger(__name__)


class XPCSClient(object):
    """Main class for dealing with SSX functions and flows

    Providers helper operations to create and cache function and flow ids.
    """

    CONF_FILE = os.path.expanduser('~') + "/.xpcs_automate.cfg"
    CLIENT_ID = "e6c75d97-532a-4c88-b031-8584a319fa3e"
    FLOW_DEFAULTS = {
            'funcx': {
                'funcx_theta': '9f84f41e-dfb6-4633-97be-b46901e9384c',
                'funcx_login': '6c4323f4-a062-4551-a883-146a352a43f5',
            },
            'transfer': {
                'aps_ep': 'fdc7e74a-fa78-11e8-9342-0e3d676669f4', #ClutchDMZ
#                'aps_ep': 'b0e921df-6d04-11e5-ba46-22000b92c6ec', #Clutch
                'proc_ep': '08925f04-569f-11e7-bef8-22000b9a448b',
                'proc_path': '/projects/APSDataAnalysis/Automate/',
            },
            'exec': {},
            'metadata': {}
        }

    def __init__(self, force_login=False, **kwargs):

        self.fxc = funcx.sdk.client.FuncXClient(
            no_local_server=kwargs.get("no_local_server", True),
            no_browser=kwargs.get("no_browser", True),
            refresh_tokens=kwargs.get("refresh_tokens", True),
            force=force_login
        )

        # Load functions and flow ids if they exist
        config = ConfigObj(self.CONF_FILE, create_empty=True)
        try:
            self.fxid_corr = config['xpcs_corr']
            self.fxid_plot = config['xpcs_plot']
            self.fxid_pilot = config['xpcs_pilot']
        except KeyError as e:
            log.debug(f'Error due to missing func: {str(e)}')
            log.debug('Registering Funcx Functions...')
            # Create if they don't exist
            self.register_functions()

        try:
            self.auto_flowid = config['automate_flowid']
            self.auto_scope = config['automate_scope']
        except KeyError as e:
            # Create if they don't exist
            self.register_flow()

    def start_flow(self, flow_input):
        """
        Initiate the flow.
        """

        flows_client = create_flows_client(self.CLIENT_ID)

        flow_action = flows_client.run_flow(self.auto_flowid, self.auto_scope, flow_input)
        flow_action_id = flow_action['action_id']
        flow_status = flow_action['status']

        return flow_action_id

    def register_functions(self):
        """Register the functions with funcx and store their ids

        Returns
        -------
        dict: phil, stills, plot
        """

        self.fxid_corr = self.fxc.register_function(xpcs_corr, description="Run CORR on .imm and .hdf")
        self.fxid_plot = self.fxc.register_function(xpcs_plot, description="Plot images")
        self.fxid_pilot = self.fxc.register_function(xpcs_pilot, description="Upload to search index")

        # Save them to the config
        config = ConfigObj(self.CONF_FILE, create_empty=True)
        config['xpcs_corr'] = self.fxid_corr
        config['xpcs_plot'] = self.fxid_plot
        config['xpcs_pilot'] = self.fxid_pilot
        config.write()

        return {'corr': self.fxid_corr, 'plot': self.fxid_plot, 'pilot': self.fxid_pilot}

    def register_flow(self):
        """Register the automate flow and store its id and scope

        Returns
        -------
        dict : id and scope
        """
        flows_client = create_flows_client(self.CLIENT_ID)

        flow = flows_client.deploy_flow(flow_definition, title="SSX Flow")
        self.auto_flowid = flow['id']
        self.auto_scope = flow['globus_auth_scope']

        config = ConfigObj(self.CONF_FILE, create_empty=True)
        config['automate_flowid'] = self.auto_flowid
        config['automate_scope'] = self.auto_scope
        config.write()

        return {'id': self.auto_flowid, 'scope': self.auto_scope}

    def create_flow_input(self, pathnames, flow_options=None):
        """
        Given a list of pathname two-tuples, creates the XPCS flow
        :param pathnames: two-tuples for the hdf and imm filenames. Ex:
            [
                (
                '/source_data/A001_Aerogel_0001-1000/A001_Aerogel_0001-1000.hdf',
                '/source_data/A001_Aerogel_0001-1000/A001_Aerogel_00001-01000.imm'
                ),
                (
                '/source_data/A002_Aerogel_0002-2000/A002_Aerogel_0002-2000.hdf',
                '/source_data/A002_Aerogel_0002-2000/A002_Aerogel_00002-02000.imm'
                )
            ]
        :param flow_options:
            see self.FLOW_DEFAULTS, all options there are overridable.
        :return:
            Globus Automate flow input for this XPCS flow.
        """
        flow_options = flow_options or {}

        options = self.FLOW_DEFAULTS.copy()
        options['funcx'].update(flow_options.get('funcx', {}))
        options['transfer'].update(flow_options.get('transfer', {}))
        options['exec'].update(flow_options.get('exec', {}))

        batch_payloads = [self.create_funcx_payload(hdf_filename, imm_filename, options)
                          for hdf_filename, imm_filename in pathnames]

        transfer = options['transfer']
        source_to_staging_transfer_items = [{
            'source_path': p['info']['source_hdf_abspath'],
            'destination_path': p['info']['proc_hdf_abspath'],
            'recursive': False
        } for p in batch_payloads] + [{
            'source_path': p['info']['source_imm_abspath'],
            'destination_path': p['info']['proc_imm_abspath'],
            'recursive': False
        } for p in batch_payloads]

        dest_to_stage_transfer_items = [{
            'source_path': p['info']['proc_hdf_abspath'],
            'destination_path': p['info']['source_hdf_abspath'].replace("cluster_results", "ALCF_results"),
            'recursive': False
        } for p in batch_payloads]


        return {
            'Transfer1Input': {
                'source_endpoint_id': transfer['aps_ep'],
                'destination_endpoint_id': transfer['proc_ep'],
                'transfer_items': source_to_staging_transfer_items
            },
            'Transfer2Input': {
                'source_endpoint_id': transfer['proc_ep'],
                'destination_endpoint_id': transfer['aps_ep'],
                'transfer_items': dest_to_stage_transfer_items
            },
            'Exec1Input': {
                'tasks': [{
                    'endpoint': options['funcx']['funcx_theta'],
                    'func': self.fxid_corr,
                    'payload': bp['payload']
                } for bp in batch_payloads]
            },
            'Exec2Input': {
                'tasks': [{
                    'endpoint': options['funcx']['funcx_theta'],
                    'func': self.fxid_plot,
                    'payload': bp['payload']
                } for bp in batch_payloads]
            },
            'Exec3Input': {
                'tasks': [{
                    'endpoint': options['funcx']['funcx_login'],
                    'func': self.fxid_pilot,
                    'payload': bp['payload'],
                } for bp in batch_payloads]
            },
        }

    def create_funcx_payload(self, hdf_pathname, imm_pathname, options=None):
        """
        Create the funcx execution payload which will run on corr, plot, and pilot.
        Creates additional metadata containing helpful pathnames.
        :param hdf_pathname: path to hdf path, on source Globus Endpoint
        :param imm_pathname: path to imm path, on source Globus Endpoint
        :param options: extra options. Uses self.FLOW_DEFAULTS for any
        options not shown
        :return:
        a dict containing 'info', for path info, and 'payload' for the payload to
        be sent to each funcx function.
        """
        options = options or self.FLOW_DEFAULTS
        # Get source pathnames
        hdf_name = os.path.basename(hdf_pathname)
        imm_name = os.path.basename(imm_pathname)
        # The dirname for the hdf is always the same as the filename minus the extension
        # Ex: foo and foo/foo.hdf
        input_parent_dir = hdf_name.replace('.hdf', '')

        # Generate Destination Pathnames.
        base_proc = os.path.join(options['transfer']['proc_path'], input_parent_dir)
        proc_hdf_file = os.path.join(base_proc, hdf_name)
        proc_imm_file = os.path.join(base_proc, imm_name)
        result_dirname = f"{input_parent_dir}/ALCF_results/{hdf_name}"
        info = {
            'hdf_filename': hdf_name,
            'imm_filename': imm_name,
            'source_hdf_abspath': hdf_pathname,
            'source_imm_abspath': imm_pathname,
            'proc_hdf_abspath': proc_hdf_file,
            'proc_imm_abspath': proc_imm_file,
            'proc_dir_abspath': os.path.dirname(proc_hdf_file),
            'proc_dirname': input_parent_dir,
            'result_abspath': result_dirname,
        }

        exec_options = options.get('exec', {})
        metadata = options.get('metadata', {})
        metadata['reprocessing'] = {
            'source_endpoint': options['transfer']['aps_ep'],
            'source_hdf_abspath': hdf_pathname,
            'source_imm_abspath': imm_pathname,
        }
        funcx_payload = {
            'data': {
                'hdf': proc_hdf_file,
                'imm': proc_imm_file,
                'metadata': metadata,
                'metadata_file': proc_hdf_file.replace(".hdf", ".json"),
            }
        }
        if 'rigaku' in exec_options:
            funcx_payload['data']['flags'] = "--rigaku"

        return {'info': info, 'payload': funcx_payload}


if __name__ == '__main__':
    kc = XPCSClient()
    pathnames = ['/tmp/clutch/foo',
               '/tmp/clutch/bar',
               '/tmp/clutch/baz',]
    res = kc.create_flow_input(pathnames)
    from pprint import pprint
    pprint(res)
