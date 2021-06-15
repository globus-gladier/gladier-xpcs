download_corr_container = {
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
      "End": True
    }
  }
}
