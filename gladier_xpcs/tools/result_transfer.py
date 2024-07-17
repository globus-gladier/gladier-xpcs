from gladier import GladierBaseTool


class ResultTransfer(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer a file or directory in Globus',
        'StartAt': 'ResultTransfer',
        'States': {
            'ResultTransfer': {
                'Comment': 'Transfer from the staging location back to the source collection',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'Parameters': {
                    'source_endpoint_id.$': '$.input.result_transfer.source_endpoint_id',
                    'destination_endpoint_id.$': '$.input.result_transfer.destination_endpoint_id',
                    'transfer_items.$': '$.input.result_transfer.transfer_items',
                },
                'ResultPath': '$.ResultTransfer',
                'WaitTime': 600,
                'End': True
            },
        }
    }

    required_input = [
        'result_transfer'
    ]
