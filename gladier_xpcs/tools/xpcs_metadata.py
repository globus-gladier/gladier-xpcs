#!/usr/bin/env python
"""
Gather metadata for an hdf file into a json file.
Usage: python xpcs_metadata.py
"""
import os
import copy
import sys
import json
import h5py
import numpy

GENERAL_METADATA = {
    "creators": [
        {
            "creatorName": "Suresh Narayanan"
        }
    ],
    "publicationYear": "2019",
    "publisher": "Argonne National Lab",
    "resourceType": {
        "resourceType": "Dataset",
        "resourceTypeGeneral": "Dataset"
    },
    "subjects": [
        {
            "subject": "beamline"
        }
    ],
}

# Keys that cause ingest into Globus Search to fail. This is likely due to
# another key of the same name being ingested previously, causing the types
# not to match (After first ingest, you cannot ingest a different type).
SPOILED_KEYS = ['measurement.instrument.source_begin.datetime']


def decode_dtype(value, dtype):
    """Update a special numpy type to a python type"""
    dt = str(dtype)
    if dt in ['uint32', 'uint64']:
        return int(value)
    elif dt in ['ufloat32', 'ufloat64', 'float32', 'float64']:
        return float(value)
    else:
        raise ValueError(f"I don't know what this is: {dt}")


def gather_items(hdf5_dataframe):
    items = {}

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
                items[key] = decode_dtype(node[0][0], node.dtype)
            if node.shape in [(1, 2), (1, 3)]:
                items[key] = node[0].tolist()

    hdf5_dataframe.visititems(gather_item)
    return items


def get_extra_metadata(metadata):
    meta = metadata.copy()

    # Add the aps_cycle_v2 key, based on another key below.
    aps_cycle_key = 'aps_cycle_v2'
    root_key = 'measurement.instrument.acquisition.root_folder'
    root = meta[root_key].lstrip('/').rstrip('/')
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
    metadata = GENERAL_METADATA.copy()
    metadata.update(gather_items(hframe))

    # Extra stuff we added in later
    extra_metadata = get_extra_metadata(metadata)
    metadata.update(extra_metadata)
    return clean_metadata(metadata, SPOILED_KEYS)



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(sys.argv)
        print(f'Usage: {sys.argv[0]} file.hdf', file=sys.stderr)
    else:
        print(json.dumps(gather(sys.argv[-1]), indent=4))