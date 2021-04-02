from gladier.defaults import GladierDefaults


def manifest_to_payload_list(data):
    """Create funcx execution tasks given a manifest"""
    import os
    import requests
    from urllib.parse import urlparse

    url = (f'https://develop.concierge.nick.globuscs.info/api/manifest/'
           f'{data["manifest_id"]}/remote_file_manifest/')
    response = requests.get(url).json()
    paths = [m['url'] for m in response['remote_file_manifest']]

    # Force all corr related files
    data['manifest_to_funcx_tasks_suffixes'] = ['hdf', 'imm', 'bin']
    if data.get('manifest_to_funcx_tasks_suffixes'):
        paths = [p for p in paths
                 if any((p.endswith(s) for s in data['manifest_to_funcx_tasks_suffixes']))]

    # Chop paths prefix
    paths = [os.path.join(os.path.basename(os.path.dirname(p)), os.path.basename(p))
             for p in paths]
    dataset_paths = {os.path.dirname(p) for p in paths}
    task_payloads = []
    for dp in dataset_paths:
        pl_data = [p for p in paths if p.startswith(dp)]
        hdf = [p for p in pl_data if p.endswith('.hdf')]
        # Purposely don't raise. This will cause corr to fail with data on which task was bad
        hdf = hdf[0] if hdf else 'No HDF File Present'
        imm = [p for p in pl_data if p.endswith('.imm') or p.endswith('.bin')]
        imm = imm[0] if imm else 'No IMM File Present'
        proc_dir = os.path.join(urlparse(data['manifest_destination']).path, dp)

        payload = {
            'proc_dir': proc_dir,
            'corr_loc': data['corr_loc'],
            'reprocessing_suffix': data['reprocessing_suffix'],
            'hdf_file': os.path.join(proc_dir, os.path.basename(hdf)),
            'imm_file': os.path.join(proc_dir, os.path.basename(imm)),
            'qmap_file': os.path.join(proc_dir, data['qmap_file']),
            'flat_file': os.path.join(proc_dir, data['flat_file']),
        }
        task_payloads.append(payload)
    return task_payloads


def mock_task(data):
    return data


def list_to_fx_tasks(data):
    funcx_state_input = {}
    for state in data['states']:
        tasks = [{
                'endpoint': state['funcx_endpoint'],
                'func': state['funcx_id'],
                'payload': pl
            } for pl in data['payloads']]

        funcx_state_input[state['name']] = {
            'tasks': tasks
        }
    return funcx_state_input


class XPCSManifestTool(GladierDefaults):
    flow_definition = {
        'Comment': 'XPCS Reprocessing Flow',
        'StartAt': 'ManifestTransfer',
        'States': {
            'ManifestTransfer': {
                'Comment': 'Transfer the contents of a manifest. Manifests MUST conform to '
                           'dir/filename spec and contain hdf/imm files.',
                'Type': 'Action',
                'ActionUrl': 'https://develop.concierge.nick.globuscs.info/api/automate/transfer',
                'ActionScope': 'https://auth.globus.org/scopes/524361f2-e4a9-4bd0-a3a6-03e365cac8a9/concierge',
                'Parameters': {
                    'manifest_id.$': '$.input.manifest_id',
                    'destination.$': '$.input.manifest_destination',
                },
                'ResultPath': '$.ManifestTransfer',
                'WaitTime': 300,
                'Next': 'ManifestToList',
            },
            'ManifestToList': {
                'Comment': 'Fetch a manifest, extract a list of files to process, and create a list of '
                           'fx payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.manifest_to_payload_list_funcx_id',
                        'payload.$': '$.input'
                    }]
                },
                'ResultPath': '$.ManifestToListResult',
                # "ResultPath": "$.result",
                'WaitTime': 300,
                # 'End': True,
                'Next': 'ListToFuncXStateTasks',
            },
            'ListToFuncXStateTasks': {
                'Comment': 'Build Qmap Payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.list_to_fx_tasks_funcx_id',
                        'payload': {
                            'states': [
                                {
                                    'name': 'ApplyQmaps',
                                    'funcx_endpoint.$': '$.input.funcx_endpoint_compute',
                                    'funcx_id.$': '$.input.apply_qmap_funcx_id',
                                },
                                {
                                    'name': 'Corr',
                                    'funcx_endpoint.$': '$.input.funcx_endpoint_compute',
                                    'funcx_id.$': '$.input.eigen_corr_funcx_id',
                                },
                                {
                                    'name': 'MakeCorrPlots',
                                    'funcx_endpoint.$': '$.input.funcx_endpoint_compute',
                                    'funcx_id.$': '$.input.make_corr_plots_funcx_id',
                                },
                                {
                                    'name': 'CustomPilot',
                                    'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                                    'funcx_id.$': '$.input.custom_pilot_funcx_id',
                                }
                            ],
                            'payloads.$': '$.ManifestToListResult.details.result'
                        }
                    }]
                },
                'ResultPath': '$.FuncXStateTasks',
                'WaitTime': 300,
                'Next': 'TransferQmap',
                # 'Next': 'CustomPilot',
                # 'End': True
            },
            "TransferQmap": {
              "Comment": "Initial Transfer from APS to ALCF",
              "Type": "Action",
              "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
              "Parameters": {
                "source_endpoint_id.$": "$.input.qmap_source_endpoint", 
                "destination_endpoint_id.$": "$.input.qmap_destination_endpoint",
                "transfer_items": [
                 {
                  "source_path.$": "$.input.qmap_source_path",
                  "destination_path.$": "$.input.qmap_file",
                  "recursive": False
                 }
               ]
              },              
              "ResultPath": "$.TransferQMapResult",
              "WaitTime": 600,
              "Next": "ApplyQmaps"
            },
            'ApplyQmaps': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.FuncXStateTasks.details.result.ApplyQmaps',
                'ResultPath': '$.ApplyQmapOutput',
                'WaitTime': 3600,
                'Next': 'Corr',
                # 'End': True,
            },
            'Corr': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.FuncXStateTasks.details.result.Corr',
                'ResultPath': '$.CorrResultsOutput',
                'WaitTime': 3600,
                'Next': 'MakeCorrPlots',
                # 'End': True,
            },
            'MakeCorrPlots': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.FuncXStateTasks.details.result.MakeCorrPlots',
                'ResultPath': '$.MakeCorrPlotsOutput',
                'WaitTime': 3600,
                # 'End': True,
                'Next': 'CustomPilot'
            },
            'CustomPilot': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.FuncXStateTasks.details.result.CustomPilot',
                'ResultPath': '$.CustomPilot',
                'WaitTime': 300,
                'End': True,
            },

        }
    }

    required_input = [
        'manifest_id',
        'manifest_destination',
        'funcx_endpoint_compute',
        'funcx_endpoint_non_compute',
    ]

    flow_input = {}

    funcx_functions = [
        manifest_to_payload_list,
        list_to_fx_tasks,
        mock_task,
    ]
