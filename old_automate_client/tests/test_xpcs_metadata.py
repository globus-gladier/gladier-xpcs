import numpy

from XPCS.tools.xpcs_metadata import clean_metadata


def test_clean_metadata_good_data():
    data = {
        'foo': 'bar',
        'moo': 3,
    }
    assert data == clean_metadata(data, [])


def test_clean_metadata_spoiled():
    data = {
        'foo': 'bar',
        'moo': 3,
    }
    assert clean_metadata(data, ['foo']) == {'moo': 3}


def test_clean_metadata_nan_vals():
    data = {
        'foo': 'bar',
        'moo': numpy.nan,
    }
    assert clean_metadata(data, []) == {'foo': 'bar', 'moo': 0}
