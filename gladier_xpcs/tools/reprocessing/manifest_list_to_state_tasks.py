from gladier import GladierBaseTool, generate_flow_definition


def list_to_fx_tasks(**data):
    funcx_state_input = {}
    for state in data['states']:
        tasks = [{
                'endpoint': state['funcx_endpoint'],
                'func': state['funcx_id'],
                'payload': pl
            } for pl in data['payloads']]

        funcx_state_input[state['name']] = {
            'tasks': tasks
        }
    return funcx_state_input


@generate_flow_definition(modifiers={
    list_to_fx_tasks: {'endpoint': 'funcx_endpoint_non_compute'},
})
class ManifestListToStateTasks(GladierBaseTool):

    flow_input = {}

    required_input = [
        'manifest_id',
    ]

    funcx_functions = [list_to_fx_tasks]
