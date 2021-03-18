flow_definition = {
  "Comment": "Automate XPCS",
  "StartAt": "Transfer1",
  "States": {
    "Transfer1": {
      "Comment": "Initial Transfer from APS to ALCF",
      "Type": "Action",
      "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
      "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
      "InputPath": "$.Transfer1Input",
      "ResultPath": "$.Transfer1Result",
      "WaitTime": 6000,
      "Next": "ExecCorr"
    },
    "ExecCorr": {
      "Comment": "Use corr to process the data",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
      "InputPath": "$.Exec1Input",
      "ResultPath": "$.Exec1Result",
      "WaitTime": 12000,
      "Next": "Transfer2"
    },
    "Transfer2": {
      "Comment": "Return data from ALCF to APS",
      "Type": "Action",
      "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
      "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
      "InputPath": "$.Transfer2Input",
      "ResultPath": "$.Transfer2Result",
      "WaitTime": 6000,
      "Next": "ExecPlots"
    },
    "ExecPlots": {
      "Comment": "Generate plots from the data",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
      "InputPath": "$.Exec2Input",
      "ResultPath": "$.Exec2Result",
      "WaitTime": 12000,
      "Next": "ExecPilot"
    },
    "ExecPilot": {
      "Comment": "Publish plots and metadata",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
      "InputPath": "$.Exec3Input",
      "ResultPath": "$.Exec3Result",
      "WaitTime": 12000,
      "End": True
    },
  }
}