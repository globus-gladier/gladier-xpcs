from gladier import GladierBaseTool


class TransferToProc(GladierBaseTool):

    flow_definition = {
        'Comment': 'Transfer files from a source endpoint into a proc or processing '
                   'location. Includes two top level transfer files for the hdf and '
                   'imm files (also .bin or .hdf for rigaku jobs) used by corr.',
        'StartAt': 'TransferToProc',
        'States': {
            'TransferToProc': {
                'Comment': 'Transfer from Clutch to Theta',
                'Type': 'Action',
                'ActionUrl': 'https://actions.automate.globus.org/transfer/transfer',
                'Parameters': {
                    'source_endpoint_id.$': '$.input.globus_endpoint_source',
                    'destination_endpoint_id.$': '$.input.globus_endpoint_proc',
                    'transfer_items': [
                        {
                            'source_path.$': '$.input.hdf_file_source',
                            'destination_path.$': '$.input.hdf_file',
                        },
                        {
                            'source_path.$': '$.input.imm_file_source',
                            'destination_path.$': '$.input.imm_file',
                        }
                    ],
                },
                'ResultPath': '$.TransferToProc',
                'WaitTime': 600,
                'End': True
            },
        }
    }

    required_input = [
        'globus_endpoint_source',
        'globus_endpoint_proc',
        'hdf_file_source',
        'hdf_file',
        'imm_file_source',
        'imm_file'
    ]