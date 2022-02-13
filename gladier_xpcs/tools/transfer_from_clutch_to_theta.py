from gladier import GladierBaseTool


class TransferFromClutchToTheta(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer a file or directory in Globus',
        'StartAt': 'TransferFromClutchToTheta',
        'States': {
            'TransferFromClutchToTheta': {
                'Comment': 'Transfer from Clutch to Theta',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'Parameters': {
                    'source_endpoint_id.$': '$.input.globus_endpoint_clutch',
                    'destination_endpoint_id.$': '$.input.globus_endpoint_theta',
                    'transfer_items.$': '$.input.transfer_from_clutch_to_theta_items',
                },
                'ResultPath': '$.TransferFromClutchToTheta',
                'WaitTime': 1800,
                'End': True
            },
        }
    }

    required_input = [
        'globus_endpoint_clutch',
        'globus_endpoint_theta',
    ]
