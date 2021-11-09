from gladier import GladierBaseTool, generate_flow_definition


def gather_xpcs_metadata(**event):
    import pathlib
    import re
    import traceback
    import datetime
    from gladier_xpcs.tools.xpcs_metadata import gather

    # Generate metadata
    hdf_file = event['hdf_file']
    exp_name = pathlib.Path(hdf_file).name.replace(".hdf", "")
    metadata = gather(hdf_file)
    metadata.update({
            'description': f'{exp_name}: Automated data processing.',
            'creators': [{'creatorName': '8-ID'}],
            'publisher': 'Automate',
            'title': exp_name,
            'subjects': [{'subject': s} for s in exp_name.split('_')],
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

    try:
        # Get root_folder, ex: "/data/2020-1/sanat202002/"
        root_folder = pathlib.Path(metadata['measurement.instrument.acquisition.root_folder'])
        # Cycle: 2021-1
        metadata['cycle'] = root_folder.parent.name
        # Parent: sanat
        metadata['parent'] = re.search(r'([a-z]+)*', root_folder.name).group()
    except Exception:
        p = pathlib.Path(event['proc_dir']) / 'gather_xpcs_metadata.error'
        with open(str(p), 'w+') as f:
            f.write(traceback.format_exc())


    pilot = event['pilot']
    # metadata passed through from the top level takes precedence. This allows for
    # overriding fields through $.input
    metadata.update(pilot.get('metadata', {}))
    pilot['metadata'] = metadata
    pilot['groups'] = pilot.get('groups', [])
    return pilot


@generate_flow_definition(modifiers={
    gather_xpcs_metadata: {'endpoint': 'funcx_endpoint_non_compute',
                           # Not supported yet
                           'ExceptionOnActionFailure': True
                           }
})
class GatherXPCSMetadata(GladierBaseTool):

    required_input = [
        'proc_dir',
        'hdf_file',
        'pilot',
    ]

    funcx_functions = [
        gather_xpcs_metadata
    ]