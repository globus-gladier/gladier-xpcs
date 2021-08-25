from gladier import GladierBaseTool, generate_flow_definition


def publish_preparation(**event):
    """Rename proc_dir and hdf_file with the reprocessing_suffix,
    delete the qmap file, and add reprocessing metadata to pilot"""
    import pathlib

    # Delete the qmap file (it's just clutter, we don't want to upload it)
    if event.get('delete_qmap') is True:
        pathlib.Path(event.get('qmap_file')).unlink(missing_ok=False)

    # .../A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf
    hdf = pathlib.Path(event['hdf_file'])
    # A001_Aerogel_1mm_att6_Lq0_001_0001-1000<reprocessing_suffix>
    new_hdf_name = f'{str(hdf.with_suffix("").name)}{event["reprocessing_suffix"]}'

    proc_dir = pathlib.Path(event['proc_dir'])
    # Rename the HDF first before the dataset (parent) directory changes
    # .../A001_Aerogel_1mm_att6_Lq0_001_0001-1000<reprocessing_suffix>.hdf
    new_hdf = hdf.rename(proc_dir / hdf.with_stem(new_hdf_name))
    # Rename the parent dataset directory
    new_proc_dir = proc_dir.rename(proc_dir.parent / new_hdf.with_suffix('').name)
    # Prepend the new dataset (parent) directory to the HDF Filename
    new_hdf_with_new_proc_dir = new_proc_dir / new_hdf.name

    names = {
        'proc_dir': str(proc_dir),
        'new_proc_dir': str(new_proc_dir),
        'new_hdf_with_new_dataset_dir': str(new_hdf_with_new_proc_dir),
        'hdf': str(hdf),
        'new_hdf': str(new_hdf),
        'new_hdf_name': str(new_hdf_name),
    }
    if not new_proc_dir.exists() or not new_hdf_with_new_proc_dir.exists():
        raise FileNotFoundError(f'File does not exist after rename: {names}')

    # Update metadata
    pilot = event['pilot']
    pilot_metadata = pilot.get('metadata', {})
    pilot_metadata.update({
        'reprocessing': {
            'qmap': {
                'source_endpoint': event['qmap_source_endpoint'],
                'source_path': event['qmap_source_path'],
                'name': pathlib.Path(event['qmap_source_path']).name,
            },
            'original': {
                'dataset': pathlib.Path(event['hdf_file']).parent.name,
                'endpoint': event['globus_endpoint_source'],
                'hdf_file': event['hdf_file_source'],
                'imm_file': event['imm_file_source'],
            },
            'suffix': event['reprocessing_suffix'],
        }
    })
    pilot['metadata'] = pilot_metadata
    pilot['dataset'] = str(new_proc_dir)
    event.update({
        'proc_dir': str(new_proc_dir),
        'hdf_file': str(new_hdf_with_new_proc_dir),
        'pilot': pilot
    })
    return event


@generate_flow_definition(modifiers={
    publish_preparation: {'endpoint': 'funcx_endpoint_non_compute',
                          # Not yet supported
                          # 'ExceptionOnActionFailure': True
                          }
})
class PublishPreparation(GladierBaseTool):

    flow_input = {
        'reprocessing_suffix': '_qmap',
        'delete_qmap': True,
    }

    required_input = [
        'proc_dir',
        'hdf_file',
        'reprocessing_suffix',
        'funcx_endpoint_non_compute',

        # Reprocessing metadata
        'qmap_source_endpoint',
        'qmap_source_path',
        'globus_endpoint_source',
        'imm_file_source',
        'hdf_file_source',

        # This is needed to hand metadata off to the GatherXPCSData step
        # and then publish after that.
        'pilot',
    ]

    funcx_functions = [
        publish_preparation
    ]
