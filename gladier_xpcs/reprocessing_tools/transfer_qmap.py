from gladier import GladierBaseTool


class TransferQmap(GladierBaseTool):

    flow_definition = {
      'Comment': 'Transfer a QMAP HDF file so it can be applied before running CORR',
      'StartAt': 'TransferQmap',
      'States': {
          "TransferQmap": {
              "Comment": "Transfer qmap from a source area to the destination",
              "Type": "Action",
              "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
              "Parameters": {
                  "source_endpoint_id.$": "$.input.qmap_source_endpoint",
                  "destination_endpoint_id.$": "$.input.globus_endpoint_proc",
                  "transfer_items": [
                      {
                          "source_path.$": "$.input.qmap_source_path",
                          "destination_path.$": "$.input.qmap_file",
                          "recursive": False
                      }
                  ]
              },
              "ResultPath": "$.TransferQMap",
              "WaitTime": 600,
              "End": True,
          },
      }
    }

    required_input = [
        # Reuse
        'globus_endpoint_proc',

        'qmap_source_endpoint',
        'qmap_source_path',
        'qmap_file',
    ]

    funcx_functions = []
