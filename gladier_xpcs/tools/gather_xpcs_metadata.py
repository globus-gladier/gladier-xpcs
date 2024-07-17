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


    def get_extra_metadata(metadata):
        meta = metadata.copy()

        # Add the aps_cycle_v2 key, based on another key below.
        aps_cycle_key = 'aps_cycle_v2'
        root_key = 'measurement.instrument.acquisition.root_folder'
        root = meta.get(root_key)
        if root:
            root = root.lstrip('/').rstrip('/')
            #_, aps_cycle, user_str = root.split('/')
            if root[-1] == '/':
                root = root[:-1]
            aps_cycle, user_str = root.split('/')[-2:]
            meta[aps_cycle_key] = '/'.join((aps_cycle, user_str))

        # Return the new metadata
        return meta


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
        extra_metadata = get_extra_metadata(metadata)
        metadata.update(extra_metadata)
        # Keys that cause ingest into Globus Search to fail. This is likely due to
        # another key of the same name being ingested previously, causing the types
        # not to match (After first ingest, you cannot ingest a different type).
        spoiled_keys = ['measurement.instrument.source_begin.datetime']
        return clean_metadata(metadata, spoiled_keys)


    # Generate metadata
    hdf_file = data['hdf_file']
    exp_name = pathlib.Path(hdf_file).name.replace(".hdf", "")
    project_metadata = gather(hdf_file)
    dc_metadata = {
        'descriptions': [{
            "description": f"{exp_name}: Automated data processing.",
            "descriptionType": "Other"
        }],
        'creators': [{'creatorName': '8-ID'}],
        'publisher': 'Automate',
        'titles': exp_name,
        'subjects': [{'subject': s} for s in exp_name.split('_')],
        'publicationYear': f'{datetime.datetime.now().year}',
        'resourceType': {
            'resourceType': 'Dataset',
            'resourceTypeGeneral': 'Dataset'
        },
        'dates': [],
        'formats': [],
        'version': "2",
    }
    extra_metadata = data.get('metadata', {}) or {}
    project_metadata.update(extra_metadata)
    # Create metadata file
    # Some types have changed between search ingests, and they cause the search ingest
    # to fail. Pop them so we don't get the search error.
    for evil_key in ['exchange.partition_norm_factor']:
        if evil_key in project_metadata.keys():
            project_metadata.pop(evil_key)

    # Get root_folder, ex: "/data/2020-1/sanat202002/"
    # All datasets need this info to publish correctly, not having it will raise an exception.
    root_folder = pathlib.Path(project_metadata['measurement.instrument.acquisition.root_folder'])
    # Cycle: 2021-1
    project_metadata['cycle'] = root_folder.parent.name
    # Parent: sanat
    project_metadata['parent'] = re.search(r'([a-z]+)*', root_folder.name).group()

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

    return {
        "destination": f'{project_metadata["cycle"]}/{root_folder.name}',
        "metadata_file": str(metadata_file),
        "cycle": project_metadata["cycle"],
        "parent": root_folder.name,
        "dataset": hdf_file
    }


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
        'proc_dir': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000',
        # 'proc_dir':'/eagle/APSDataAnalysis/nick/xpcs_gpu',
        'hdf_file': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/output/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
        'execution_metadata_file': 'foo/bar',
        'boost_corr': {
            'gpu_flag': 0
        },
        'publishv2': {
            'metadata': {},
            'groups': []
        },
    }
    from pprint import pprint
    pprint(gather_xpcs_metadata(**data))
