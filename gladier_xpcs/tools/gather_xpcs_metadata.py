from gladier import GladierBaseTool, generate_flow_definition


def gather_xpcs_metadata(**data):
    import pathlib
    import re
    import traceback
    import datetime
    import os
    import json
    import h5py
    import copy
    import numpy

    # These are the keys we collect with version 2 of the metadata
    # Version 2 refers to July 2024, when 8idi first started to run
    # datasets with the new beamline coming online.
    V2_XPCS_KEYS = [
        'aps_cycle_v2',
        'cycle',
        'entry.duration',
        'entry.end_time',
        'entry.entry_identifier',
        'entry.instrument.bluesky.metadata.I0',
        'entry.instrument.bluesky.metadata.I1',
        'entry.instrument.bluesky.metadata.X_energy',
        'entry.instrument.bluesky.metadata.absolute_cross_section_scale',
        'entry.instrument.bluesky.metadata.acquire_period',
        'entry.instrument.bluesky.metadata.acquire_time',
        'entry.instrument.bluesky.metadata.bcx',
        'entry.instrument.bluesky.metadata.bcy',
        'entry.instrument.bluesky.metadata.beamline_id',
        'entry.instrument.bluesky.metadata.ccdx',
        'entry.instrument.bluesky.metadata.ccdx0',
        'entry.instrument.bluesky.metadata.ccdy',
        'entry.instrument.bluesky.metadata.ccdy0',
        'entry.instrument.bluesky.metadata.concise',
        'entry.instrument.bluesky.metadata.dataDir',
        'entry.instrument.bluesky.metadata.data_management',
        'entry.instrument.bluesky.metadata.databroker_catalog',
        'entry.instrument.bluesky.metadata.datetime',
        'entry.instrument.bluesky.metadata.description',
        'entry.instrument.bluesky.metadata.det_dist',
        'entry.instrument.bluesky.metadata.detector_name',
        'entry.instrument.bluesky.metadata.detectors',
        'entry.instrument.bluesky.metadata.header',
        'entry.instrument.bluesky.metadata.hints',
        'entry.instrument.bluesky.metadata.incident_beam_size_nm_xy',
        'entry.instrument.bluesky.metadata.incident_energy_spread',
        'entry.instrument.bluesky.metadata.index',
        'entry.instrument.bluesky.metadata.instrument_name',
        'entry.instrument.bluesky.metadata.login_id',
        'entry.instrument.bluesky.metadata.metadatafile',
        'entry.instrument.bluesky.metadata.num_capture',
        'entry.instrument.bluesky.metadata.num_exposures',
        'entry.instrument.bluesky.metadata.num_images',
        'entry.instrument.bluesky.metadata.num_intervals',
        'entry.instrument.bluesky.metadata.num_points',
        'entry.instrument.bluesky.metadata.num_triggers',
        'entry.instrument.bluesky.metadata.owner',
        'entry.instrument.bluesky.metadata.pid',
        'entry.instrument.bluesky.metadata.pix_dim_x',
        'entry.instrument.bluesky.metadata.pix_dim_y',
        'entry.instrument.bluesky.metadata.plan_args',
        'entry.instrument.bluesky.metadata.plan_name',
        'entry.instrument.bluesky.metadata.plan_type',
        'entry.instrument.bluesky.metadata.proposal_id',
        'entry.instrument.bluesky.metadata.qmap_file',
        'entry.instrument.bluesky.metadata.safe_title',
        'entry.instrument.bluesky.metadata.t0',
        'entry.instrument.bluesky.metadata.t1',
        'entry.instrument.bluesky.metadata.title',
        'entry.instrument.bluesky.metadata.versions',
        'entry.instrument.bluesky.metadata.workflow',
        'entry.instrument.bluesky.metadata.xdim',
        'entry.instrument.bluesky.metadata.ydim',
        'entry.instrument.bluesky.streams.primary.eiger4M.image_file_name',
        'entry.instrument.detector_1.description',
        'entry.instrument.layout_version',
        'entry.program_name',
        'entry.start_time',
        'entry.title',
        'parent',
        'project-slug',
        'xpcs.analysis_type',
        'xpcs.avg_frame_burst',
        'xpcs.avg_frames',
        'xpcs.dnophi',
        'xpcs.dnoq',
        'xpcs.qmap_hdf5_filename',
        'xpcs.snophi',
        'xpcs.snoq',
        'xpcs.stride_frame_burst',
        'xpcs.stride_frames']

    def gather_items(hdf5_dataframe):
        def decode_dtype(key, value, dtype):
            """Update a special numpy type to a python type"""
            dt = str(dtype)
            if dt in ['uint32', 'uint64', 'int32', 'int64']:
                return int(value)
            elif dt in ['ufloat32', 'ufloat64', 'float32', 'float64']:
                return float(value)
            else:
                raise ValueError(f"Field {key} returned unexpected type {dt} for value {value}.")

        def gather_item(name, node):
            if isinstance(node, h5py.Dataset):
                key = name.replace('/', '.')
                if node.shape == ():
                    try:
                        items[key] = node[()].decode('utf-8')
                    except Exception:
                        try:
                            items[key] = node[()].item()
                        except Exception:
                            items[key] = node[()]
                if node.shape == (1, 1):
                    items[key] = decode_dtype(key, node[0][0], node.dtype)
                if node.shape in [(1, 2), (1, 3)]:
                    items[key] = node[0].tolist()
        items = {}
        hdf5_dataframe.visititems(gather_item)
        return items


    def clean_metadata(metadata, spoiled_keys):
        """Change or delete metadata that meets the following criteria:
        * Value is NAN --
            * Reason: Cannot ingest NAN into Globus Search
            * Result: Change to zero.
        * Value in SPOILED_KEYS
            * Reason: Value type has changed in Globus Search since previous ingest
            * Result: Remove key entirely.
        """
        meta = copy.deepcopy(metadata)

        for key in spoiled_keys:
            if key in meta.keys():
                meta.pop(key)

        for key, val in meta.items():
            if any([isinstance(val, t) for t in [int, float]]):
                if numpy.isnan(val):
                    meta[key] = 0

        return meta


    def gather(dataframe):
        hframe = h5py.File(dataframe, 'r')

        metafilename, _ = os.path.splitext(os.path.basename(dataframe))
        metafilename += '.json'
        metadata = dict()
        metadata.update(gather_items(hframe))

        # Extra stuff we added in later
        extra_metadata = dict() # get_extra_metadata(metadata)
        metadata.update(extra_metadata)
        # Keys that cause ingest into Globus Search to fail. This is likely due to
        # another key of the same name being ingested previously, causing the types
        # not to match (After first ingest, you cannot ingest a different type).
        spoiled_keys = ['measurement.instrument.source_begin.datetime']
        return clean_metadata(metadata, spoiled_keys)


    # Generate metadata
    hdf_file = data['hdf_file']
    exp_name = pathlib.Path(hdf_file).name.replace(".hdf", "")
    gathered_metadata = gather(hdf_file)
    dc_metadata = {
        'descriptions': [{
            "description": f"{exp_name}: Automated data processing.",
            "descriptionType": "Other"
        }],
        'creators': [{'creatorName': '8-ID'}],
        'publisher': 'Automate',
        'titles': [{'title': exp_name}],
        'subjects': [{'subject': s} for s in exp_name.split('_')],
        'publicationYear': f'{datetime.datetime.now().year}',
        'resourceType': {
            'resourceType': 'Dataset',
            'resourceTypeGeneral': 'Dataset'
        },
        'dates': [
            {
                # "date": "2020-09-04T21:08:59.027046Z",
                # "date": "2024-07-17T16:01:36.595827",
                "date": datetime.datetime.now().isoformat(),
                "dateType": "Created"
            }
        ],
        'formats': [],
        'version': "2",
    }
    extra_metadata = data.get('metadata', {}) or {}
    project_metadata = extra_metadata.copy()
    # Create metadata file
    # Some types have changed between search ingests, and they cause the search ingest
    # to fail. Pop them so we don't get the search error.
    for evil_key in ['exchange.partition_norm_factor']:
        if evil_key in project_metadata.keys():
            project_metadata.pop(evil_key)

    # Get root_folder, ex: "/data/2020-1/sanat202002/"
    # All datasets need this info to publish correctly, not having it will raise an exception.
    # /gdata/dm/8IDI/2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000
    # root_folder = pathlib.Path(gathered_metadata['entry.instrument.bluesky.metadata.dataDir'])
    # This is a quick fix while we're fixing metadata, so we can continue to publish stuff
    root_folder = pathlib.Path("/gdata/dm/8IDI/2025-1/foster/data/current_xpcs_dataset")
    # 2024-1/zhang202402_2/data/H001_27445_QZ_XPCS_test-01000
    relative_folder = root_folder.relative_to('/gdata/dm/8IDI/')
    # ("2024-1", "zhang202402_2", "data", H001_27445_QZ_XPCS_test-01000)
    aps_parts = relative_folder.parts
    exp_metadata = {
        # Cycle: 2021-1
        'cycle': aps_parts[0],
        # Parent: zhang
        'parent': re.search(r'([a-z]+)*', aps_parts[1]).group(),
        # Raw Cycle/Parent: 2021-1/sanat012345
        'aps_cycle_v2': f'{aps_parts[0]}/{aps_parts[1]}',
        # This is an old pilot-era publish v1 fixture which should be removed once the portal filters are
        # removed.
        "project-slug": "xpcs-8id",
    }
    project_metadata.update(exp_metadata)
    wanted_gathered_metadata = {k:v for k, v in gathered_metadata.items() if k in V2_XPCS_KEYS}
    project_metadata.update(wanted_gathered_metadata)
    unexpected_xpcs_keys = [k for k in gathered_metadata if k not in V2_XPCS_KEYS]


    if os.path.exists(data['execution_metadata_file']):
        with open(data['execution_metadata_file']) as f:
            project_metadata.update(json.load(f))
        os.unlink(data['execution_metadata_file'])

    metadata = {
        "dc": dc_metadata,
        "project_metadata": project_metadata,
    }

    metadata_file = pathlib.Path(hdf_file).parent / "xpcs_metadata.json"
    with open(metadata_file, 'w') as f:
        json.dump(metadata, f, indent=2)


    # Update the publish data with a couple extra key pieces of info
    new_data = {
        # Add nested folders to destination
        "destination": str(pathlib.Path(data["publishv2"]["destination"]) / project_metadata["aps_cycle_v2"]),
        "metadata_file": str(metadata_file),
        "unexpected_xpcs_keys": unexpected_xpcs_keys,
    }
    publish_data = data['publishv2']
    publish_data.update(new_data)
    return publish_data


@generate_flow_definition(modifiers={
    gather_xpcs_metadata: {'endpoint': 'login_node_endpoint',
                           'ExceptionOnActionFailure': True,
                           }
})
class GatherXPCSMetadata(GladierBaseTool):

    required_input = [
        'proc_dir',
        'hdf_file',
        'publishv2',
    ]

    compute_functions = [
        gather_xpcs_metadata
    ]


if __name__ == '__main__':
    data = {
        #'proc_dir': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000',
        'proc_dir': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/H001_27445_QZ_XPCS_test-01000',
        # 'proc_dir':'/eagle/APSDataAnalysis/nick/xpcs_gpu',
        #'hdf_file': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/output/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
        'hdf_file': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/H001_27445_QZ_XPCS_test-01000/output/H001_27445_QZ_XPCS_test-01000.hdf',
        'execution_metadata_file': 'foo/bar',
        'boost_corr': {
            'gpu_flag': 0
        },
        'publishv2': {
            'metadata': {},
            'groups': [],
            'destination': '/foo/bar',
        },
    }
    from pprint import pprint
    pprint(gather_xpcs_metadata(**data))
