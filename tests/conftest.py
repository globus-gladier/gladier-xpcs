import pytest
import pathlib
from unittest.mock import Mock
import gladier

def pytest_configure(config):
    """Configure to allow the @pytest.mark.slow option"""
    config.addinivalue_line(
        "markers", "slow: mark test to run only on named environment"
    )
    config.log_cli = True

def pytest_addoption(parser):
    """Add this as a possible option when running pytest"""
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )

def pytest_collection_modifyitems(config, items):
    """Only run tests marked as slow if the --runslow option is set."""
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


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
