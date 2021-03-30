from gladier.defaults import GladierDefaults


def manifest_to_reprocessing_task(data):
    """Create funcx execution tasks given a manifest"""
    import os
    import requests
    from urllib.parse import urlparse

    required = [
        'manifest_to_funcx_tasks_manifest_id',
        'manifest_to_funcx_tasks_funcx_endpoint_compute',
        'manifest_to_funcx_tasks_callback_funcx_id',
    ]
    missing = [r for r in required if r not in data.keys()]
    if any(missing):
        raise ValueError(f'{missing} inputs MUST be included')

    url = (f'https://develop.concierge.nick.globuscs.info/api/manifest/'
           f'{data["manifest_to_funcx_tasks_manifest_id"]}/remote_file_manifest/')
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
        payload = {
            'proc_dir': urlparse(data['manifest_destination']).path,
            'corr_loc': data['corr_loc'],
            'hdf_file': hdf,
            'imm_file': imm,
            'flat_file': data['flat_file'],
        }
        task_payloads.append(payload)
    return task_payloads

    # # if data.get('manifest_to_funcx_tasks_use_dirs') is True:
    # #     paths = list({os.path.dirname(p) for p in paths})
    # tasks = []
    # for path in paths:
    #     task = dict(
    #         endpoint=data['manifest_to_funcx_tasks_funcx_endpoint_compute'],
    #         func=data['manifest_to_funcx_tasks_callback_funcx_id'],
    #     )
    #     # 'proc_dir': '/lus/theta-fs0/projects/APSDataAnalysis/nick/xpcs',
    #     # 'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
    #     # 'hdf_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
    #     # 'imm_file': 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm',
    #     # 'flags': '',
    #     # 'qmap_file': 'sanat201903_qmap_S270_D54_lin.h5',
    #     # 'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',
    #     purl = urlparse(path)
    #     task['payload'] = data.get('manifest_to_funcx_tasks_payload', {})
    #     task['payload']['protocol'] = purl.scheme
    #     task['payload']['host'] = purl.netloc
    #     task['payload']['path'] = purl.path
    #
    #     tasks.append(task)
    # return {'tasks': tasks}

def mock_task(data):
    return 'it worked!'


def to_fx_payloads(data):
    return {'tasks': [{
                'endpoint': data['funcx_endpoint'],
                'func': data['funcx_id'],
                'payload': pl
        }] for pl in data['payloads']}


class XPCSManifestTool(GladierDefaults):
    flow_definition = {
        'Comment': 'XPCS Reprocessing Flow',
        'StartAt': 'GetReprocessingPayloads',
        'States': {
            # 'ManifestTransfer': {
            #     'Comment': 'Transfer the contents of a manifest. Manifests MUST conform to '
            #                'dir/filename spec and contain hdf/imm files.',
            #     'Type': 'Action',
            #     'ActionUrl': 'https://develop.concierge.nick.globuscs.info/api/automate/transfer',
            #     'ActionScope': 'https://auth.globus.org/scopes/524361f2-e4a9-4bd0-a3a6-03e365cac8a9/concierge',
            #     'Parameters': {
            #         'manifest_id.$': '$.input.manifest_id',
            #         'destination.$': '$.input.manifest_destination',
            #     },
            #     'ResultPath': '$.ManifestTransfer',
            #     'WaitTime': 300,
            #     'Next': 'CreateFuncXTasks',
            # },
            'GetReprocessingPayloads': {
                'Comment': 'Fetch a manifest, extract a list of files to process, and create a list of '
                           'fx payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        # 'func.$': '$.input.manifest_to_reprocessing_task_funcx_id',
                        'func.$': '$.input.manifest_to_reprocessing_task_funcx_id',
                        'payload.$': '$.input'
                    }]
                },
                'ResultPath': '$.ReprocessingPayloads',
                # "ResultPath": "$.result",
                'WaitTime': 300,
                # 'End': True,
                'Next': 'ApplyQmapPayloads',
            },
            'ApplyQmapPayloads': {
                'Comment': 'Build Qmap Payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.to_fx_payloads_funcx_id',
                        'payload': {
                            'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                            'funcx_id.$': '$.input.mock_task_funcx_id',
                            # 'funcx_id.$': '$.input.apply_qmap_funcx_id',
                            'payloads.$': '$.ReprocessingPayloads.details.result'
                        }
                    }]
                },
                'ResultPath': '$.ApplyQmapPayloads',
                'WaitTime': 300,
                'Next': 'CorrPayloads',
            },
            'CorrPayloads': {
                'Comment': 'Fetch a manifest, extract a list of files to process, and create a list of '
                           'fx payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.to_fx_payloads_funcx_id',
                        'payload': {
                            'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                            'funcx_id.$': '$.input.mock_task_funcx_id',
                            # 'funcx_id.$': '$.input.eigen_corr_funcx_id',
                            'payloads.$': '$.ReprocessingPayloads.details.result'
                        }
                    }]
                },
                'ResultPath': '$.CorrPayloads',
                'WaitTime': 300,
                'Next': 'MakeCorrPlotsPayload',
            },
            'MakeCorrPlotsPayload': {
                'Comment': 'Fetch a manifest, extract a list of files to process, and create a list of '
                           'fx payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.to_fx_payloads_funcx_id',
                        'payload': {
                            'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                            'funcx_id.$': '$.input.mock_task_funcx_id',
                            # 'funcx_id.$': '$.input.make_corr_plots_funcx_id',
                            'payloads.$': '$.ReprocessingPayloads.details.result'
                        }
                    }]
                },
                'ResultPath': '$.MakeCorrPlotsPayload',
                'WaitTime': 300,
                'Next': 'CustomPilotPayloads',
            },
            'CustomPilotPayloads': {
                'Comment': 'Fetch a manifest, extract a list of files to process, and create a list of '
                           'fx payloads',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.to_fx_payloads_funcx_id',
                        'payload': {
                            'funcx_endpoint.$': '$.input.funcx_endpoint_non_compute',
                            'funcx_id.$': '$.input.mock_task_funcx_id',
                            # 'funcx_id.$': '$.input.custom_pilot_funcx_id',
                            'payloads.$': '$.ReprocessingPayloads.details.result'
                        }
                    }]
                },
                'ResultPath': '$.CustomPilotPayloads',
                'WaitTime': 300,
                # 'Next': 'Corr',
                'End': True,
            },

            'Corr': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.CorrPayloads.details.result',
                'ResultPath': '$.CorrResultsOutput',
                'WaitTime': 300,
                'End': True,
            },
            'ApplyQmaps': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.ApplyQmapPayloads.details.result',
                'ResultPath': '$.ApplyQmapOutput',
                'WaitTime': 300,
                'End': True,
            },
            'MakeCorrPlots': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.MakeCorrPlotsPayload.details.result',
                'ResultPath': '$.MakeCorrPlotsOutput',
                'WaitTime': 300,
                'End': True,
            },
            'CustomPilot': {
                'Comment': 'Run CORR on the previously defined payload',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                # Fetch the input generated by the previous task
                'InputPath': '$.CustomPilotPayloads.details.result',
                'ResultPath': '$.CustomPilot',
                'WaitTime': 300,
                'End': True,
            },

        }
    }

    required_input = [
        'funcx_endpoint_non_compute',
        'manifest_to_funcx_tasks_manifest_id',

        'manifest_to_funcx_tasks_funcx_endpoint_compute',
        'manifest_to_funcx_tasks_callback_funcx_id'
    ]

    flow_input = {
        # Contains tutorial files on /share/godata/ for Globus Tutorial Endpoint 1
        'manifest_id': '80cae0bb-fe9c-4f91-ac03-93e1ac550b7e',
        # By default, this will transfer an hdf and imm pair
        'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/projects/APSDataAnalysis/Automate/reprocessing/',
        # Contains tutorial files on /share/godata/ for Globus Tutorial Endpoint 1
        'manifest_to_funcx_tasks_manifest_id': '80cae0bb-fe9c-4f91-ac03-93e1ac550b7e',
        'manifest_to_funcx_tasks_funcx_endpoint_compute': '4b116d3c-1703-4f8f-9f6f-39921e5864df',
        'manifest_to_funcx_tasks_callback_funcx_id': None,

    }

    funcx_functions = [
        manifest_to_reprocessing_task,
        to_fx_payloads,
        mock_task,
    ]
