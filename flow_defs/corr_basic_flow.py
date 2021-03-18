
corr_basic_flow_definition = {
  "Comment": "Eigen Base Test",
  "StartAt": "Eigen Corr",
  "States": {
    "Eigen Corr": {
      "Comment": "Eigen Corr",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_ep",
            "func.$": "$.input.eigen_corr_funcx_id",
            "payload": {
              "proc_dir.$": "$.input.proc_dir",
              "hdf_file.$": "$.input.hdf",
              "imm_file.$": "$.input.imm",
              "flags.$": "$.input.flags"
              "corr_loc.$": "$.input.corr_loc"
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "End": True
    }
  }
}