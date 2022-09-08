import pytest
from unittest.mock import patch
from scripts import get_status


@pytest.mark.parametrize(
    'flows_status_response, expected_result',
    [
        # Top level
        ({'state_name': 'MyStateName'}, 'MyStateName'),
        # Double nested under details
        ({
            'details': {
                'details': {
                    'state_name': 'MyStateName'
                }
            }
        }, 'MyStateName'),
        # Double nested under details inside output.
        ({
            'details': {
                'details': {
                    'output': {
                        'MyStateName': {
                            'state_name': 'MyStateName',
                        },
                    }
                }
            }
        }, 'MyStateName'),
        # Double nested under details, but no state_name key
        ({
            'details': {
                'details': {
                    'output': {
                        'MyStateName': {},
                    }
                }
            }
        }, 'MyStateName'),
        # details.action_statuses
        ({
            'details': {
                'action_statuses': [
                    {
                        'state_name': 'MyStateName',
                    },
                ]
            }
        }, 'MyStateName'),
        # details.FlowStarting
        ({
            'details': {
                'code': 'FlowStarting',
            }
        }, None),
    ],
)
def test_get_current_state_name(flows_status_response, expected_result):
    assert get_status.get_current_state_name(flows_status_response) == expected_result


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
