import pytest
import json
import pathlib

from gladier_xpcs.tools.gather_xpcs_metadata import gather_xpcs_metadata

VALID_PILOT_METADAA = pathlib.Path(__file__).parent / "publish_v1_2024-02-20.json"

TEST_INPUTS = {
    'proc_dir': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000',
    # 'proc_dir':'/eagle/APSDataAnalysis/nick/xpcs_gpu',
    'hdf_file': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/output/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
    'metadata_file': '/Users/nick/globus/aps/xpcs_client/gladier_xpcs/tools/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/output/xpcs_metadata.json',
    'execution_metadata_file': 'foo/bar',
    'boost_corr': {
        'gpu_flag': 0
    },
    'publishv2': {
        'metadata': {},
        'groups': []
    },
}

@pytest.fixture
def valid_metadata():
    with open(VALID_PILOT_METADAA) as f:
        return json.loads(f.read())


def test_publish(valid_metadata):
    from pprint import pprint
    valid_metadata["content"].pop("files")
    gather = gather_xpcs_metadata(**TEST_INPUTS)
    metadata = json.loads(pathlib.Path(TEST_INPUTS["metadata_file"]).read_text())

    for metadata in (
            valid_metadata["content"],
            metadata
            ):
        assert set(metadata) == {"dc", "project_metadata"}
        assert set(metadata["dc"]) == {'descriptions', 'creators', 'publisher', 'titles', 'subjects', 'publicationYear', 'resourceType', 'dates', 'formats', 'version'}
        pm = metadata["project_metadata"]
        xpcs = [m for m in pm if m.startswith('xpcs')]
        measurement = [m for m in pm if m.startswith('measurement')]
        other = [m for m in pm if not m.startswith('measurement') and not m.startswith('xpcs')]

        assert len(xpcs) == 50
        assert len(measurement) == 68
        assert set(other).intersection({"aps_cycle_v2", "cycle", "parent"})
