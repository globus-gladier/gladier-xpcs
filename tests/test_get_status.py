from unittest.mock import patch
from scripts import get_status


@patch('builtins.print')
@patch('pprint.pprint')
@patch('sys.exit')
def test_unexpected_automate_response(print, pprint, exit):
    rc = get_status.get_current_state_name({})
    assert rc is None
    assert print.called
    assert pprint.called


@patch('builtins.print')
@patch('pprint.pprint')
@patch('sys.exit')
def test_unexpected_automate_response_no_state_in_details(print, pprint, exit):
    status_response = {'details': {'details': {'this': 'shouldnt be here'}}}
    rc = get_status.get_current_state_name(status_response)
    assert rc is None
    assert print.called
    assert pprint.called
