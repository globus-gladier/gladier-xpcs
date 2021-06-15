reprocess_flow = {
  "Comment": "Eigen Flow",
  "StartAt": "Download Container",
  "States": {
    "Download Container": {
      "Comment": "Download the container",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_local_ep",
            "func.$": "$.input.https_download_fxid",
            "payload": {
              "server_url.$": "$.input.container_server_url",
              "file_name.$": "$.input.container_name",
              "file_path.$": "$.input.container_path",
              "headers.$": "$.input.headers"
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "Next": "Download Data"
    },
    "Download Data": {
      "Comment": "Download the data",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_local_ep",
            "func.$": "$.input.https_download_fxid",
            "payload": {
              "server_url.$": "$.input.dataset_server_url",
              "file_name.$": "$.input.dataset_name",
              "file_path.$": "$.input.data_dir",
              "headers.$": "$.input.headers"
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "Next": "Unzip Data"
    },
    "Unzip Data": {
      "Comment": "Unzip the data",
      "Type": "Action",
      "ActionUrl": "https://api.funcx.org/automate",
      "ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all",
      "Parameters": {
          "tasks": [{
            "endpoint.$": "$.input.funcx_local_ep",
            "func.$": "$.input.unzip_data_fxid",
            "payload": {
              "file_name.$": "$.input.dataset_name",
              "file_path.$": "$.input.data_dir",
              "output_path.$": "$.input.proc_dir"
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
              "proc_dir.$": "$.input.proc_dir",
              "hdf_file.$": "$.input.hdf",
              "imm_file.$": "$.input.imm",
              "flags.$": "$.input.flags"
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
            "func.$": "$.input.qmap_fxid",
            "payload": {
              "proc_dir.$": "$.input.proc_dir",
              "input_file.$": "$.input.hdf",
              "qmap_file.$": "$.input.qmap",
              "flat_file.$": "$.input.flatfield",
              "output_file.$": "$.input.output",
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
              "proc_dir.$": "$.input.proc_dir",
              "hdf_file.$": "$.input.output",
              "imm_file.$": "$.input.imm",
              "flags.$": "$.input.flags"
            }
        }]
      },
      "ResultPath": "$.result",
      "WaitTime": 600,
      "End": True
    }
  }
}