from gladier import GladierBaseTool

class BoostCorrRuntime(GladierBaseTool):

    flow_definition = {
        "StartAt": "BoostCorrRuntime",
        "States": {
            "BoostCorrRuntime": {
                "Type": "ExpressionEval",
                "Parameters": {
                    "tools.$": "$.XpcsBoostCorr.details.results[0].output.metadata.tools",
                },
                "ResultPath": "$.input.webplot_extra_metadata",
                "End": True
                }
        }
    }
