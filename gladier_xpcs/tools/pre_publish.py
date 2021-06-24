"""
This is a copy of the publication tool in order to make pre-publication
possible. Duplicating tools currently is not supported, but it may be
supported by the time you read this! Checkout:

https://github.com/globus-gladier/gladier/pull/91

and

https://github.com/globus-gladier/gladier/blob/master/CHANGELOG.md

And probably docs here:

https://gladier.readthedocs.io/en/stable/?badge=stable

If the PR is merged, the changelog supports tool duplication, and the docs
show how to do it, you should probably delete this file!
"""

from gladier import GladierBaseTool, generate_flow_definition


def pre_publish_gather_metadata(data):
    from pilot.client import PilotClient

    dataset, destination = data['dataset'], data.get('destination', '/')
    index, project, groups = data['index'], data['project'], data.get('groups', [])

    # Bootstrap Pilot
    pc = PilotClient(config_file=None, index_uuid=index)
    pc.project.set_project(project)
    # short_path is how pilot internally refers to datasets, implicitly accounting for
    # the endpoint and base project path. After publication, you may refer to your
    # dataset via the short path -- ``pilot describe short_path``
    short_path = pc.build_short_path(dataset, destination)
    return {
        'search': {
            'id': data.get('id', 'metadata'),
            'content': pc.gather_metadata(dataset, destination,
                                          custom_metadata=data.get('metadata')),
            'subject': pc.get_subject_url(short_path),
            'visible_to': [f'urn:globus:groups:id:{g}' for g in groups + [pc.get_group()]],
            'search_index': index
        },
        'transfer': {
            'source_endpoint_id': data['source_globus_endpoint'],
            'destination_endpoint_id': pc.get_endpoint(),
            'transfer_items': [{
                'source_path': src,
                'destination_path': dest,
                # 'recursive': False,  # each file is explicit in pilot, no directories
            } for src, dest in pc.get_globus_transfer_paths(dataset, destination)]
        }
    }


class PrePublish(GladierBaseTool):

    flow_definition = {
        'Comment': 'Publish metadata to Globus Search, with data from the result.',
        'StartAt': 'PrePublishGatherMetadata',
        'States': {
            'PrePublishGatherMetadata': {
                'Comment': 'Say something to start the conversation',
                'Type': 'Action',
                'ActionUrl': 'https://api.funcx.org/automate',
                'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2',
                'ExceptionOnActionFailure': False,
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'func.$': '$.input.pre_publish_gather_metadata_funcx_id',
                        'payload.$': '$.input.pilot',
                    }]
                },
                'ResultPath': '$.PrePublishGatherMetadata',
                'WaitTime': 60,
                'Next': 'PrePublishTransfer',
            },
            'PrePublishTransfer': {
                'Comment': 'Transfer files for publication',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'InputPath': '$.PrePublishGatherMetadata.details.result.transfer',
                'ResultPath': '$.PrePublishTransfer',
                'WaitTime': 600,
                'Next': 'PrePublishIngest',
            },
            'PrePublishIngest': {
                'Comment': 'Ingest the search document',
                'Type': 'Action',
                'ActionUrl': 'https://actions.globus.org/search/ingest',
                'ExceptionOnActionFailure': False,
                'InputPath': '$.PrePublishGatherMetadata.details.result.search',
                'ResultPath': '$.PrePublishIngest',
                'WaitTime': 300,
                'End': True
            },
        }
    }

    required_input = [
        'pilot',
        'funcx_endpoint_non_compute',
    ]

    flow_input = {

    }

    funcx_functions = [
        pre_publish_gather_metadata,
    ]
