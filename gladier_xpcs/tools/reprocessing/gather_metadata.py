from gladier import GladierBaseTool


def custom_pilot(event):
    import os
    import json
    import shutil
    import datetime
    from XPCS.tools.xpcs_metadata import gather
    from pilot.client import PilotClient

    import json
    with open(event['parameter_file']) as f:
        event = json.load(f)

    # Do the last minute renaming before uploading.
    # The proc dir is a ../A001_Aerogel_1mm_att6_Lq0_001_0001-1000
    base_proc_dir = os.path.dirname(event['proc_dir'])
    # Split A001_Aerogel_1mm_att6_Lq0_001_0001-1000 and .hdf
    dataset_name, extension = os.path.splitext(os.path.basename(event['hdf_file']))
    # A001_Aerogel_1mm_att6_Lq0_001_0001-1000
    old_dataset_directory = os.path.join(base_proc_dir, os.path.dirname(event['hdf_file']))
    # A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap
    new_dataset_directory = os.path.join(base_proc_dir, f'{dataset_name}{event["reprocessing_suffix"]}')
    # A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf'
    old_hdf_name = os.path.join(new_dataset_directory, os.path.basename(event['hdf_file']))
    # A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap/A001_Aerogel_1mm_att6_Lq0_001_0001-1000_qmap.hdf
    new_hdf_name = os.path.join(new_dataset_directory, f'{dataset_name}{event["reprocessing_suffix"]}{extension}')

    # return {
    #     'proc_dir': event['proc_dir'],
    #     'hdf_file': event['hdf_file'],
    #     'dataset_name': dataset_name,
    #     'old_dataset_directory': old_dataset_directory,
    #     'new_dataset_directory': new_dataset_directory,
    #     'old_hdf_name': old_hdf_name,
    #     'new_hdf_name': new_hdf_name,
    # }
    os.rename(old_dataset_directory, new_dataset_directory)
    os.rename(old_hdf_name, new_hdf_name)
    hdf_file = new_hdf_name

    # Generate metadata
    upload_dir = os.path.dirname(hdf_file)
    exp_name = os.path.basename(hdf_file).replace(".hdf", "")
    metadata = gather(hdf_file)
    metadata.update({
            'description': f'{exp_name}: Automated data processing.',
            'creators': [{'creatorName': '8-ID'}],
            'publisher': 'Automate',
            'title': exp_name,
            'subjects': [{'subject': 'XPCS'}, {'subject': '8-ID'}],
            'publicationYear': f'{datetime.datetime.now().year}',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            }
        })
    extra_metadata = event.get('metadata', {}) or {}
    metadata.update(extra_metadata)
    # Create metadata file
    # Some types have changed between search ingests, and they cause the search ingest
    # to fail. Pop them so we don't get the search error.
    for evil_key in ['exchange.partition_norm_factor']:
        if evil_key in metadata.keys():
            metadata.pop(evil_key)

    # ONLY upload images, this is required for using the test server
    # We can remove this when petrel is back up and running
    # upload_dir = os.path.join(exp_dir, exp_name)
    # if not os.path.exists(upload_dir):
    #     os.mkdir(upload_dir)
    # for image in [img for img in os.listdir(exp_dir) if img.endswith('.png')]:
    #     shutil.copy(os.path.join(exp_dir, image), os.path.join(upload_dir, image))


class CustomPilot(GladierBaseTool):

    flow_definition = {
      'Comment': 'Run Pilot and upload the result to search + petreldata',
      'StartAt': 'CustomPilot',
      'States': {
        'CustomPilot': {
          'Comment': 'Upload to petreldata, ingest to xpcs search index',
          'Type': 'Action',
          'ActionUrl': 'https://api.funcx.org/automate',
          'ActionScope': 'https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/all',
          'Parameters': {
              'tasks': [{
                'endpoint.$': '$.input.funcx_endpoint_non_compute',
                'func.$': '$.input.custom_pilot_funcx_id',
                'payload.$': '$.input',
            }]
          },
          'ResultPath': '$.result',
          'WaitTime': 600,
          'End': True
        }
      }
    }

    flow_input = {
        'reprocessing_suffix': '_qmap',
    }

    required_input = [
        'proc_dir',
        'hdf_file',
        'reprocessing_suffix',
    ]

    funcx_functions = [
        custom_pilot
    ]
