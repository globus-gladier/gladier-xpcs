import pytest
import pathlib
from unittest.mock import Mock
import gladier


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--integration"):
        # --integration given in cli: do not skip slow tests
        return
    integration_test = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(integration_test)


# @pytest.fixture(autouse=True)
# def mock_gladier_client(monkeypatch):
#     monkeypatch.setattr(gladier.GladierBaseClient, 'is_logged_in', Mock(return_value=True))
#     monkeypatch.setattr(gladier.GladierBaseClient, 'get_input',
#                         Mock(return_value={'input': {}}))
#     return gladier.GladierBaseClient


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
