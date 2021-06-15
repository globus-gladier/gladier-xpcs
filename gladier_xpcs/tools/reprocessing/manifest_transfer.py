from gladier import GladierBaseTool


class ManifestTransfer(GladierBaseTool):

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
                'WaitTime': 3600,  # 1 hour wait time
                'End': True,
            },
        }
    }

    flow_input = {}

    required_input = [
        'manifest_id',
        'manifest_destination',
    ]

    funcx_functions = []
