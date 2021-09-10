from unittest.mock import patch, mock_open
from gladier_xpcs.tools.gather_xpcs_metadata import gather_xpcs_metadata


def test_gather_xpcs_metadata(mock_gather, reprocessing_runtime_input):
    response = gather_xpcs_metadata(**reprocessing_runtime_input)
    for item in ['source_globus_endpoint', 'index', 'metadata']:
        assert item in response

    pm = response['metadata']
    assert pm['title'] == 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000'
    assert pm['measurement.instrument.acquisition.root_folder'] == '/data/2020-1/sanat202002/'
    assert pm['parent'] == 'sanat'
    assert pm['cycle'] == '2020-1'


def test_bad_parent(mock_gather, reprocessing_runtime_input):
    del mock_gather.return_value['measurement.instrument.acquisition.root_folder']
    with patch('builtins.open', mock_open()) as mock_file:
        response = gather_xpcs_metadata(**reprocessing_runtime_input)
    assert 'parent' not in response['metadata']
    assert 'cycle' not in response['metadata']
    mock_file.assert_called_with('/projects/APSDataAnalysis/xpcs/mock_staging_dir/'
                                 'A001_Aerogel_1mm_att6_Lq0_001_0001-1000/gather_xpcs_metadata.error', 'w+')
