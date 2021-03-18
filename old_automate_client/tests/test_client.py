import pytest
from XPCS.tools.client import XPCSClient


@pytest.mark.skip(reason='Need to mock function and flow registration')
def test_client_basic_payload(mock_funcx):
    kc = XPCSClient()
    files = [
        (
        '/source_data/A001_Aerogel_0001-1000/A001_Aerogel_0001-1000.hdf',
        '/source_data/A001_Aerogel_0001-1000/A001_Aerogel_00001-01000.imm'
        ),
        (
        '/source_data/A002_Aerogel_0002-2000/A002_Aerogel_0002-2000.hdf',
        '/source_data/A002_Aerogel_0002-2000/A002_Aerogel_00002-02000.imm'
        )
    ]
    options = {
        'funcx': {
            'funcx_theta': 'funcx_theta',
            'funcx_login': 'funcx_login',
        },
        'transfer': {
            'aps_ep': 'aps_globus_ep',
            'proc_ep': 'proc_globus_ep',
            'proc_path': '/processing_area/'
        }
    }
    result = kc.create_flow_input(files, options)
