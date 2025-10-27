from gladier import GladierBaseTool


class SourceTransferCleanup(GladierBaseTool):
    """
    This task cleans up transfer items on a folw
    """

    flow_definition = {
        "StartAt": "SourceTransferCleanup",
        "States": {
            "SourceTransferCleanup": {
                "Type": "ExpressionEval",
                "Parameters": {
                    "input.source_transfer.transfer_items": "DELETED",
                },
                "ResultPath": "$.input.source_transfer.transfer_items",
                "End": True
                }
        }
    }
