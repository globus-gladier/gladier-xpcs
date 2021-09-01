import pytest
import pathlib
from unittest.mock import Mock
import gladier
from gladier_xpcs.flow_reprocess import XPCSReprocessingFlow
from gladier_xpcs.deployments import BaseDeployment


@pytest.fixture(autouse=True)
def mock_gladier_client(monkeypatch):
    monkeypatch.setattr(gladier.GladierBaseClient, 'is_logged_in', Mock(return_value=True))
    monkeypatch.setattr(gladier.GladierBaseClient, 'get_input',
                        Mock(return_value={'input': {}}))
    return gladier.GladierBaseClient


@pytest.fixture
def mock_pathlib(monkeypatch):
    # Mock any pathlib methods that would actually change files
    monkeypatch.setattr(pathlib.Path, 'unlink', Mock())
    monkeypatch.setattr(pathlib.Path, 'exists', Mock(return_value=True))

    def rename(self, path):
        """Rename should return the new name of the file its renaming"""
        return path if isinstance(path, pathlib.Path) else pathlib.Path(path)
    monkeypatch.setattr(pathlib.Path, 'rename', rename)
    return pathlib.Path


@pytest.fixture
def reprocessing_deployment():
    class MockDeployment(BaseDeployment):
        globus_endpoints = {
            'globus_endpoint_source': 'pertel_or_eagle_endpoint_uuid',
            'globus_endpoint_proc': 'theta_endpoint_uuid',
        }
        funcx_endpoints = {
            'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
            'funcx_endpoint_compute': '2272d362-c13b-46c6-aa2d-bfb22255f1ba',
        }
        flow_input = {
            'input': {
                'staging_dir': '/projects/APSDataAnalysis/xpcs/mock_staging_dir',
                'funcx_endpoint_non_compute': 'funcx_endpoint_non_compute_mock',
                'funcx_endpoint_compute': 'funcx_endpoint_compute_mock',
            }
        }
    return MockDeployment()


@pytest.fixture
def reprocessing_input():
    flow_input = {
        'reprocessing_suffix': '<qmap_suffix>',
        'delete_qmap': True,

        'hdf_source': '/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                           'A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf',
        'imm_source': '/XPCSDATA/Automate/A001_Aerogel_1mm_att6_Lq0_001_0001-1000/'
                      'A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm',

        # Reprocessing metadata
        'qmap_source_endpoint': 'qmap_source_endpoint_mock',
        'qmap_source_path': '/GlobusPortal_XPCS/sanat201903_qmap_S270_D54_lin.h5',
    }
    return flow_input


@pytest.fixture
def reprocessing_runtime_input(reprocessing_deployment, reprocessing_input):
    cli = XPCSReprocessingFlow()
    xpcs_input = cli.get_xpcs_input(
        reprocessing_deployment,
        reprocessing_input['hdf_source'],
        reprocessing_input['imm_source'],
        reprocessing_input['qmap_source_path'],
    )
    xpcs_input['input'].update(reprocessing_input)
    return xpcs_input['input']
