from gladier import GladierBaseTool, generate_flow_definition


def acquire_nodes(**data):
    return 'Compute nodes have been acquired.'


@generate_flow_definition(modifiers={
    acquire_nodes: {'WaitTime': 86400}  # Wait 1 day for free nodes
})
class AcquireNodes(GladierBaseTool):
    """Acquire Nodes specifically does nothing, and is intended to be run before
    other compute tasks to ensure compute nodes are ready. This solves the problem
    of actual compute tasks timing out waiting for a node to spin up."""
    funcx_functions = [acquire_nodes]
    required_input = ['funcx_endpoint_compute']
