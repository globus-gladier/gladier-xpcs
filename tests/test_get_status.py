import pytest
from unittest.mock import patch
from scripts import get_status


@pytest.mark.parametrize(
    'flows_status_response',
    [
        # Top level
        {'state_name': 'MyStateName'},
        # Double nested under details
        {
            'details': {
                'details': {
                    'state_name': 'MyStateName'
                }
            }
        },
        # Double nested under details inside output.
        {
            'details': {
                'details': {
                    'output': {
                        'MyStateName': {
                            'state_name': 'MyStateName',
                        },
                    }
                }
            }
        },
        # Double nested under details, but no state_name key
        {
            'details': {
                'details': {
                    'output': {
                        'MyStateName': {},
                    }
                }
            }
        },
        # details.action_statuses
        {
            'details': {
                'action_statuses': [
                    {
                        'state_name': 'MyStateName',
                    },
                ]
            }
        },
    ],
)
def test_get_current_state_name(flows_status_response):
    assert get_status.get_current_state_name(flows_status_response) == 'MyStateName'


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
