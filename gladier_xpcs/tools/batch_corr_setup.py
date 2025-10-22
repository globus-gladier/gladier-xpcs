from gladier import GladierBaseTool, generate_flow_definition


def batch_corr_setup(xpcs_boost_corr_tasks: str = None, boost_corr: dict = None, qmap: str = None, **data):
    import pathlib
    import json
    import shutil
    import copy

    skipped = 0

    for task in xpcs_boost_corr_tasks:
        qmap_input = pathlib.Path(qmap)
        work_dir = pathlib.Path(task["kwargs"]["corr_input_file"])
        qmap_staging = work_dir / qmap_input.name
        output_dir = work_dir.parent / "output"
        boost_corr_input = work_dir / "boost_corr_input.json"

        try:
            boost_corr_dataset_input = copy.deepcopy(boost_corr)
            
            work_dir.mkdir(exist_ok=True, parents=True)

            files_list = sorted([str(d) for d in list(work_dir.iterdir()) if not d.name in ["boost_corr_input.json", qmap_input.name]])
            if not files_list:
                skipped += 1
                continue
                # raise Exception(f'Failed to collect input files from list: {files_list}')
            raw_file = files_list[0]
            boost_corr_dataset_input["raw"] = raw_file
            
            shutil.copyfile(qmap_input, qmap_staging)
            boost_corr_dataset_input["qmap"] = str(qmap_staging)

            output_dir.mkdir(exist_ok=True, parents=True)
            boost_corr_dataset_input["output"] = str(output_dir)

            # boost_corr_input = work_dir / "boost_corr_input.json"
            boost_corr_input.write_text(json.dumps(boost_corr_dataset_input, indent=2))
        except Exception as e:
            raise Exception({
                "Exception": str(e),
                "qmap_input": f"qmap_input: {qmap_input}, exists? {qmap_input.exists()}",
                "work_dir": f"work_dir: {work_dir}, exists? ({work_dir.exists()})",
                "qmap_staging": f"qmap_staging: {qmap_staging}, exists? {qmap_staging.exists()}",
                "output_dir": f"output_dir: {output_dir}, exists? {output_dir.exists()}",
            })

    return {
        "tasks_ready": len(xpcs_boost_corr_tasks),
        "qmap": str(qmap_staging),
        "boost_corr_input": str(boost_corr_input),
        "skipped": skipped,
    }


@generate_flow_definition(modifiers={
    batch_corr_setup: {
        'WaitTime': 604800,
        'ExceptionOnActionFailure': True,
        # "tasks": "$.input.xpcs_boost_corr_tasks",
        "user_endpoint_config": {
                # "queue": "demand",
                "queue": "debug",
                # "queue": "preemptable",
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