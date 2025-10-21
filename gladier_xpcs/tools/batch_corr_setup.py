from gladier import GladierBaseTool, generate_flow_definition


def batch_corr_setup(xpcs_boost_corr_tasks: str, boost_corr: dict, qmap: str, **data):
    import pathlib
    import json
    import shutil
    import copy

    for task in xpcs_boost_corr_tasks:
        qmap_input = pathlib.Path(qmap)
        boost_corr_dataset_input = copy.deepcopy(boost_corr)
        
        work_dir = pathlib.Path(task["kwargs"]["corr_input_file"])
        work_dir.mkdir(exist_ok=True, parents=True)

        files_list = sorted([str(d) for d in list(work_dir.iterdir()) if not d.name in ["boost_corr_input.json", qmap_input.name]])
        if not files_list:
                raise Exception(f'Failed to collect input files from list: {files_list}')
        raw_file = files_list[0]
        boost_corr_dataset_input["raw"] = raw_file
        
        qmap_staging = work_dir / qmap_input.name
        shutil.copyfile(qmap_input, qmap_staging)
        boost_corr_dataset_input["qmap"] = str(qmap_staging)

        output_dir = work_dir.parent / "output"
        output_dir.mkdir(exist_ok=True)
        boost_corr_dataset_input["output"] = str(output_dir)


        boost_corr_input = work_dir / "boost_corr_input.json"
        boost_corr_input.write_text(json.dumps(boost_corr_dataset_input, indent=2))

    return {
        "tasks_ready": len(xpcs_boost_corr_tasks)
    }


@generate_flow_definition(modifiers={
    batch_corr_setup: {
        'WaitTime': 604800,
        'ExceptionOnActionFailure': True,
        "tasks": "$.input.xpcs_boost_corr_tasks",
        "user_endpoint_config": {
                # "queue": "demand",
                "queue": "preemptable",
        }
    }
})
class BatchCorrSetup(GladierBaseTool):
    action_url = "https://compute.actions.globus.org/v3"

    required_input = [
        'boost_corr',
        'compute_endpoint',
    ]

    compute_functions = [
        batch_corr_setup
    ]