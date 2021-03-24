corr_basic_flow_definition = {
  "Comment": "Eigen Base Test",
  "StartAt": "Eigen Corr",
  "States": {
    "Eigen Corr": {
      "Comment": "Eigen Corr",
      "Type": "Action", ## automate task
      "ActionUrl": "https://api.funcx.org/automate", ## automate task
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all", ## automate task
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_ep",
            "func.$": "$.input.eigen_corr_funcx_id",
            "payload": {
              "input.$": "$.input",
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "End": True
    }
  }
}

qmap_flow_definition = {
  "Comment": "Eigen Flow",
  "StartAt": "Apply QMap",
  "States": {
    "Apply QMap": {
      "Comment": "Eigen Corr",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_ep",
            "func.$": "$.input.apply_qmap_funcx_id",
            "payload": {
              "input.$": "$.input",
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "Next": "Eigen Corr"
    },
    "Eigen Corr": {
      "Comment": "Eigen Corr",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_ep",
            "func.$": "$.input.corr_fxid",
            "payload": {
              "input.$": "$.input",
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "End": True
    }
  }
}



# ryan_flow_definition = {
#   "Comment": "Automate XPCS",
#   "StartAt": "Transfer1",
#   "States": {
#     "Transfer1": {
#       "Comment": "Initial Transfer from APS to ALCF",
#       "Type": "Action",
#       "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
#       "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
#       "InputPath": "$.Transfer1Input",
#       "ResultPath": "$.Transfer1Result",
#       "WaitTime": 6000,
#       "Next": "ExecCorr"
#     },
#     "ExecCorr": {
#       "Comment": "Use corr to process the data",
#       "Type": "Action",
#       "ActionUrl": "https://api.funcx.org/automate",
#       "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
#       "InputPath": "$.Exec1Input",
#       "ResultPath": "$.Exec1Result",
#       "WaitTime": 12000,
#       "Next": "Transfer2"
#     },
#     "Transfer2": {
#       "Comment": "Return data from ALCF to APS",
#       "Type": "Action",
#       "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
#       "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
#       "InputPath": "$.Transfer2Input",
#       "ResultPath": "$.Transfer2Result",
#       "WaitTime": 6000,
#       "Next": "ExecPlots"
#     },
#     "ExecPlots": {
#       "Comment": "Generate plots from the data",
#       "Type": "Action",
#       "ActionUrl": "https://api.funcx.org/automate",
#       "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
#       "InputPath": "$.Exec2Input",
#       "ResultPath": "$.Exec2Result",
#       "WaitTime": 12000,
#       "Next": "ExecPilot"
#     },
#     "ExecPilot": {
#       "Comment": "Publish plots and metadata",
#       "Type": "Action",
#       "ActionUrl": "https://api.funcx.org/automate",
#       "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
#       "InputPath": "$.Exec3Input",
#       "ResultPath": "$.Exec3Result",
#       "WaitTime": 12000,
#       "End": True
#     },
#   }
# }


