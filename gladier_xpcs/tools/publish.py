from gladier import GladierBaseTool, generate_flow_definition


def publish_gather_metadata(**data):
    import traceback
    from pilot.client import PilotClient
    from pilot.exc import PilotClientException, FileOrFolderDoesNotExist


    try:
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
    except (PilotClientException, FileOrFolderDoesNotExist):
        return traceback.format_exc()


class Publish(GladierBaseTool):

    flow_definition = {
        'Comment': 'Publish metadata to Globus Search, with data from the result.',
        'StartAt': 'PublishGatherMetadata',
        'States': {
            'PublishGatherMetadata': {
                'Comment': 'Say something to start the conversation',
                'Type': 'Action',
                'ActionUrl': 'https://automate.funcx.org',
                'ActionScope': 'https://auth.globus.org/scopes/b3db7e59-a6f1-4947-95c2-59d6b7a70f8c/action_all',
                'ExceptionOnActionFailure': True,
                'Parameters': {
                    'tasks': [{
                        'endpoint.$': '$.input.funcx_endpoint_non_compute',
                        'function.$': '$.input.publish_gather_metadata_funcx_id',
                        'payload.$': '$.input.pilot',
                    }]
                },
                'ResultPath': '$.PublishGatherMetadata',
                'WaitTime': 1200,
                'Next': 'PublishTransfer',
            },
            'PublishTransfer': {
                'Comment': 'Transfer files for publication',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'InputPath': '$.PublishGatherMetadata.details.result[0].transfer',
                'ResultPath': '$.PublishTransfer',
                'WaitTime': 1800,
                'Next': 'PublishIngest',
            },
            'PublishIngest': {
                'Comment': 'Ingest the search document',
                'Type': 'Action',
                'ActionUrl': 'https://actions.globus.org/search/ingest',
                'ExceptionOnActionFailure': True,
                'InputPath': '$.PublishGatherMetadata.details.result[0].search',
                'ResultPath': '$.PublishIngest',
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
        publish_gather_metadata,
    ]
