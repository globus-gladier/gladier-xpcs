from gladier import GladierBaseTool, generate_flow_definition


def warm_nodes(**data):
    return 'Nodes are warm and toasty'


@generate_flow_definition(modifiers={
    warm_nodes: {'WaitTime': 86400}  # Wait 1 day for free nodes
})
class WarmNodes(GladierBaseTool):
    """Warm Nodes specifically does nothing, and is intended to be run before
    other compute tasks to ensure compute nodes are ready. This solves the problem
    of actual compute tasks timing out waiting for a node to spin up."""
    funcx_functions = [warm_nodes]
    required_input = ['funcx_endpoint_compute']
