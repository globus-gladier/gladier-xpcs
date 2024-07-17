from gladier import GladierBaseTool


class SourceTransfer(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer a file or directory in Globus',
        'StartAt': 'SourceTransfer',
        'States': {
            'SourceTransfer': {
                'Comment': 'Transfer from the source collection to the staging location',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'Parameters': {
                    'source_endpoint_id.$': '$.input.source_transfer.source_endpoint_id',
                    'destination_endpoint_id.$': '$.input.source_transfer.destination_endpoint_id',
                    'transfer_items.$': '$.input.source_transfer.transfer_items',
                },
                'ResultPath': '$.SourceTransfer',
                'WaitTime': 60,
                'End': True
            },
        }
    }

    required_input = [
        'source_transfer'
    ]
