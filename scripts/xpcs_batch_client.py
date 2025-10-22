import globus_sdk
from pprint import pprint
import urllib
import pathlib
import json
import sys
import os

from gladier_xpcs.flows.flow_boost import XPCSBoost
from gladier import FlowsManager
from gladier_xpcs.tools.xpcs_boost_corr import xpcs_boost_corr
from gladier_xpcs.flows.flow_boost_batch import XPCSBoostBatch
from gladier_xpcs.deployments import BaseDeployment, deployment_map

from scripts import xpcs_online_boost_client


"aa2b18e8-e248-4265-985c-7e2e59765539:/8IDI/2025-2/zhang202506/data"
# /eagle/APSDataProcessing/aps8idi/

# Voyager
deployment = deployment_map["voyager-8idi-polaris"]
# collection = "aa2b18e8-e248-4265-985c-7e2e59765539"
# source_path = pathlib.Path(deployment.source_collection.to_globus("/system/test/s8iddm_boost_corr_test/data/"))
source_path = pathlib.Path(deployment.source_collection.to_globus("/8IDI/2025-2/tempus202507-merge/data/converted/Ha0277_PO2_a0002_f2000000/"))

# source_path = pathlib.Path("/8IDI/2025-2/zhang202506/data")
# source_path = pathlib.Path("/system/test/s8iddm_boost_corr_test/data/")
staging_path = pathlib.Path(deployment.get_input()["input"]["staging_dir"]) / "batch-test-2025-10-22/tempus202507-merge"
run_kwargs = {"run_monitors": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"],
              "run_managers": [f"urn:globus:auth:identity:3b843349-4d4d-4ef3-916d-2a465f9740a9"]}


app = globus_sdk.ClientApp("scripting", client_id=os.getenv("GLOBUS_CLI_CLIENT_ID"), client_secret=os.getenv("GLOBUS_CLI_CLIENT_SECRET"))
# app = globus_sdk.UserApp("scripting", client_id="ad810e6b-15ae-44cd-906e-bedff947a0da")
transfer_client = globus_sdk.TransferClient(app=app)


data = transfer_client.operation_ls(deployment.source_collection.uuid, path=source_path)

first_hundred = [d["name"] for d in data["DATA"] if d["type"] == "dir"][:2]
transfer_items = [
    {
        "source_path": str(source_path / item),
        "destination_path": str(staging_path / item / "input"),
        "recursive": True
    }
    for item in first_hundred
]
# staging_qmap = str(staging_path / "qmaps/eiger4m_qmap_1018_hongrui_d36.h5")
staging_qmap = str(staging_path / "eiger4m_qmap_1018_hongrui_d36.h5")

qmap = {
    'destination_path': staging_qmap,
    'recursive': False,
    # 'source_path': '/8IDI/2024-3/comm202410/data/eiger4m_qmap_1018_hongrui_d36.h5'
    # 'source_path': str(deployment.source_collection.to_globus('/system/test/s8iddm_boost_corr_test/data/rigaku_qmap_Sq360_Dq36_Lin_0501.hdf')),
    'source_path': str(deployment.source_collection.to_globus('/8IDI/2025-2/tempus202507-merge/data/timepix_Sq90_Dq9_log.hdf')),
}
transfer_items.append(qmap)


# dataset = {
#         "dataset_type": "Eiger4m",
#         "type": "Twotime",
#         "size": "404 MB",
#         "gpu_id": -1,
#         "raw": "/gdata/dm/8IDI/2025-2/weichen202506/data/A0067_SIO2_P50_Load1_att_10_5_2_1_Quiescent_a0005_f002000_r00001/A0067_SIO2_P50_Load1_att_10_5_2_1_Quiescent_a0005_f002000_r00001.h5",
#         "qmap": "/gdata/dm/8IDI/2025-2/weichen202506/data/eiger4m_qmap_default.hdf",
#         "experiment": "integration-test202507",
#         "cycle": "2025-2",
# }

def get_boost_corr_defaults():
    """
    Boost Corr Arguments mostly come from the input section, and is something we can't easily mock.

    The main things we overwrite from these values are 'raw', 'qmap', and 'type'. Output typically isn't
    used since we don't want to transfer these back to Voyager.

    Other values we don't tend to change.
    """
    return {
        "avg_frame": 1,
        "begin_frame": 0,
        "dq_selection": "all",
        "end_frame": -1,
        "overwrite": True,
        # "qmap": "",
        # "raw": "",
        "save_g2": False,
        "smooth": "sqmap",
        "stride_frame": 1,
        "verbose": True,

        "type": "Multitau",
        "gpu_id": 0,
        # "output": "",
    }

# def _get_flow_input(dataset, boost_corr_arguments):
#     deployment = get_deployment(DEPLOYMENT, dataset["raw"])
#     filepaths = get_filepaths(
#         dataset["raw"],
#         dataset["qmap"],
#         "",
#         dataset["experiment"],
#         deployment,
#         cycle=dataset["cycle"],
#     )

#     boost_corr_arguments.update(
#         {
#             "qmap": filepaths["qmap"]["compute"],
#             "raw": filepaths["raw"]["compute"],
#             "output": filepaths["output"]["compute"]["directory"],
#             "type": dataset["type"],
#             # By default use GPU. -1 for CPU
#             "gpu_id": dataset.get("gpu_id", 0),
#         }
#     )

#     metadata = get_extra_metadata(dataset["experiment"], dataset["cycle"])
#     flow_input = get_flow_input(
#         deployment,
#         filepaths,
#         boost_corr_arguments,
#         skip_transfer_back=True,
#         additional_groups=[],
#         extra_metadata=metadata,
#     )

#     return flow_input

from pprint import pprint 
flows_manager = FlowsManager(run_kwargs=run_kwargs)
batch_flow = XPCSBoostBatch(flows_manager=flows_manager)
fids = batch_flow.get_compute_function_ids()
# pprint(fids)
if not fids.get("xpcs_boost_corr_function_id"):
    print("WARNING: No xpcs_boost_corr_function_id function found!")
# See if we can pack 100 datasets into a transfer
flow_input = {
    "input": {
        # Old 8idi Polaris endpoint
        # "compute_endpoint": "f8f4692a-0ab7-40d0-b256-ba5b82b5e2ec",
        # Ryan's new ALCF MEP
        "compute_endpoint": "d88919ea-026a-493e-9124-fe3c46defa54",
        # Webplot xpcs
        # "compute_endpoint": "48f5f3d6-dde5-4431-bb5f-8bc01e1cb386",
        # Webplot gce-test3
        # "compute_endpoint": "c66b4066-4a39-46ab-be95-216517d9234f",
        # Tutorial (I think)
        # "compute_endpoint": "4b116d3c-1703-4f8f-9f6f-39921e5864df",
        "staging_base_path": str(staging_path),
        "staging_qmap": staging_qmap,
        "boost_corr": get_boost_corr_defaults(),
        "qmap": qmap["destination_path"],
                #     {
        #         'function_id.$': '$.input.xpcs_boost_corr_function_id',
        #         'kwargs': {'dir.$': '$.input.source_transfer.transfer_items[0].destination_path'},
        #     },
        "xpcs_boost_corr_tasks": [
            {
                "function_id": fids["xpcs_boost_corr_function_id"],
                'kwargs': {'corr_input_file': d["destination_path"]},
            } for d in transfer_items[:-1]
        ],
        'source_transfer': {
            'destination_endpoint_id': '98d26f35-e5d5-4edd-becf-a75520656c64',
            'source_endpoint_id': 'aa2b18e8-e248-4265-985c-7e2e59765539',
            'transfer_items': transfer_items
        },
    },
}

pprint(batch_flow.get_flow_definition())
pprint(flow_input)
label = f"{source_path.parts[3]}-test"
run = batch_flow.run_flow(flow_input=flow_input, label=label, tags=['aps', 'xpcs', 'batch-test'])
status = batch_flow.progress(run["run_id"])
pprint(batch_flow.get_status(run["run_id"]))
pprint(status)

