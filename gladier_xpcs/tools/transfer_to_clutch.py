from gladier import GladierBaseTool


class TransferToClutch(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer the processed file back to clutch',
        'StartAt': 'TransferToClutch',
        'States': {
            'TransferToClutch': {
                'Comment': 'Transfer from Theta to Clutch',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'ExceptionOnActionFailure': True,
                'Parameters': {
                    'source_endpoint_id.$': '$.input.globus_endpoint_theta',
                    'destination_endpoint_id.$': '$.input.globus_endpoint_clutch',
                    'transfer_items.$': '$.input.transfer_to_clutch',
                },
                'ResultPath': '$.TransferToClutch',
                'WaitTime': 600,
                'End': True
            },
        }
    }

    required_input = [
        'globus_endpoint_clutch',
        'globus_endpoint_theta',
        'transfer_to_clutch'
    ]
