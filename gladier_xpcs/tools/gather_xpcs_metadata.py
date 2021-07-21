from gladier import GladierBaseTool, generate_flow_definition


def gather_xpcs_metadata(**event):
    import os
    import datetime
    from gladier_xpcs.tools.xpcs_metadata import gather

    # Generate metadata
    hdf_file = event['hdf_file']
    exp_name = os.path.basename(hdf_file).replace(".hdf", "")
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

    pilot = event['pilot']
    pilot['metadata'] = metadata
    pilot['groups'] = pilot.get('groups', [])
    return pilot


@generate_flow_definition(modifiers={
    gather_xpcs_metadata: {'endpoint': 'funcx_endpoint_non_compute'}
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