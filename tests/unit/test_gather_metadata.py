import pytest
import json
import pathlib
import h5py
from unittest.mock import Mock, mock_open

from gladier_xpcs.tools.gather_xpcs_metadata import gather_xpcs_metadata

TEST_INPUTS = {
    "proc_dir": "/base_folder/A001_Aerogel",
    "hdf_file": "/base_folder/A001_Aerogel/output/A001_Aerogel.hdf",
    "execution_metadata_file": "foo/bar",
    "metadata": {
        "entry.start_time": "2025-03-17 05:23:39.251266",
        "entry.end_time": "2025-03-17 05:23:39.251273",
    },
    "publishv2": {
        "destination": "/foo/bar",
    },
    "source_transfer": {
        "transfer_items": [
            {
                "source_path": "/2025-1/milliron202503/data/volfrac116029_10nm-frac_a0050_f100000_r00001/volfrac116029_10nm-frac_a0050_f100000_r00001.bin.003",
            }
        ]
    },
}


@pytest.fixture
def mock_hdf(monkeypatch):
    monkeypatch.setattr(h5py, "File", Mock())
    return h5py.File


@pytest.fixture
def mock_open_fixture(monkeypatch):
    m = mock_open()
    monkeypatch.setattr("builtins.open", m)
    return m


def test_gather_metadata_destination_from_source_transfer(mock_hdf, mock_open_fixture):
    gather = gather_xpcs_metadata(**TEST_INPUTS)
    assert gather["destination"] == "/foo/bar/2025-1/milliron202503"
