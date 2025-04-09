from gladier import GladierBaseTool


class ResultTransfer(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer a file or directory in Globus',
        'StartAt': 'ResultTransferChoice',
        'States': {
            "ResultTransferChoice": {
                "Comment": "Determine if the document should be cataloged in Globus Search",
                "Type": "Choice",
                "Choices": [
                    {
                        "And": [
                            {
                                "Variable": "$.input.enable_result_transfer",
                                "IsPresent": True,
                            },
                            {
                                "Variable": "$.input.enable_result_transfer",
                                "BooleanEquals": True,
                            },
                        ],
                        "Next": "ResultTransferDoTransfer",
                    }
                ],
                "Default": "ResultTransferSkipTransfer",
            },
            'ResultTransferDoTransfer': {
                'Comment': 'Transfer from the staging location back to the source collection',
                'Type': 'Action',
                'ActionUrl': 'https://transfer.actions.globus.org/transfer/',
                'Parameters': {
                    'source_endpoint.$': '$.input.result_transfer.source_endpoint_id',
                    'destination_endpoint.$': '$.input.result_transfer.destination_endpoint_id',
                    'DATA.$': '$.input.result_transfer.transfer_items',
                },
                'ResultPath': '$.ResultTransferDoTransfer',
                'WaitTime': 86400,
                "Next": "ResultTransferDone",
            },
            "ResultTransferSkipTransfer": {
                "Comment": "The Result Transfer step has been skipped",
                "Type": "Pass",
                "Next": "ResultTransferDone",
            },
            "ResultTransferDone": {
                "Comment": "Result Transfer has finished execution",
                "Type": "Pass",
                "End": True,
            },
        }
    }

    flow_input = {
        'enable_result_transfer': False,
    }

    required_input = [
        'result_transfer',
        'enable_result_transfer'
    ]
