from unittest.mock import Mock
import pytest

import funcx.sdk.client


@pytest.fixture
def mock_funcx(monkeypatch):
    monkeypatch.setattr(funcx.sdk.client, 'FuncXClient', Mock())
    return funcx.sdk.client.FuncXClient
