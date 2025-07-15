import pytest
from gladier import FlowsManager
import argparse
from gladier_xpcs.flows.flow_boost import XPCSBoost
from scripts.xpcs_online_boost_client import (
    get_deployment,
    get_filepaths,
    get_extra_metadata,
    get_flow_input,
    get_boost_corr_arguments,
    arg_parse,
    start_flow,
)
import logging

log = logging.getLogger(__name__)


DEPLOYMENT = "voyager-8idi-polaris"
DATASETS = [
    {
        "dataset_type": "Rigaku3m",
        "type": "Multitau",
        "raw": "/gdata/dm/8IDI/2025-1/milliron202503/data/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001.bin.000",
        "qmap": "/gdata/dm/8IDI/2025-1/milliron202503/data/rigaku_qmap_Sq360_Dq36_Lin_0501.hdf",
        "experiment": "milliron202503",
        "cycle": "2025-1",
    },
    # {
    #     "dataset_type": "Eiger4m",
    #     "raw": "/gdata/dm/8IDI/2025-2/olsen202506b/data/Da0194_P4-5wv-35C_a0008_f004000_r00001/",
    #     "qmap": "/gdata/dm/8IDI/2025-2/olsen202506b/data/eiger4m_qmap_S360_D36_Lin.hdf"
    # }
]


@pytest.fixture
def boost_corr_arguments():
    return {
        "avg_frame": 1,
        "begin_frame": 0,
        "dq_selection": "all",
        "end_frame": -1,
        "gpu_id": 0,
        # 'output': '/eagle/APSDataProcessing/aps8idi/xpcs_staging/milliron202503/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001/output/rigaku_qmap_Sq360_Dq36_Lin_0501/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001',
        "overwrite": True,
        # 'qmap': '/eagle/APSDataProcessing/aps8idi/xpcs_staging/milliron202503/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001/qmap/rigaku_qmap_Sq360_Dq36_Lin_0501.hdf',
        # 'raw': '/eagle/APSDataProcessing/aps8idi/xpcs_staging/milliron202503/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001/input/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001.bin.000',
        "save_g2": False,
        "smooth": "sqmap",
        "stride_frame": 1,
        "type": "Multitau",
        "verbose": True,
    }


def _get_flow_input(dataset, boost_corr_arguments):

    deployment = get_deployment(DEPLOYMENT, dataset["raw"])
    filepaths = get_filepaths(
        dataset["raw"],
        dataset["qmap"],
        "",
        dataset["experiment"],
        deployment,
        cycle=dataset["cycle"],
    )

    boost_corr_arguments.update(
        {
            "qmap": filepaths["qmap"]["compute"],
            "raw": filepaths["raw"]["compute"],
            "output": filepaths["output"]["compute"]["directory"],
        }
    )

    metadata = get_extra_metadata(dataset["experiment"], dataset["cycle"])
    flow_input = get_flow_input(
        deployment,
        filepaths,
        boost_corr_arguments,
        skip_transfer_back=True,
        additional_groups=[],
        extra_metadata=metadata,
    )

    return flow_input


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset", DATASETS, ids=lambda d: f"{d['dataset_type']} -- {d['type']}"
)
def test_datasets(dataset, boost_corr_arguments):

    flow_input = _get_flow_input(dataset, boost_corr_arguments)

    run_kwargs = {"run_monitors": flow_input["input"]["publishv2"]["visible_to"]}
    flows_manager = FlowsManager(run_kwargs=run_kwargs)

    corr_flow = XPCSBoost(flows_manager=flows_manager)
    dataset_name = f"{dataset['dataset_type']} -- {dataset['type']}"

    assert corr_flow.get_flow_id()
    assert "xpcs_boost_corr_function_id" in corr_flow.get_input()["input"]
    result = corr_flow.run_flow(
        flow_input=flow_input, label=dataset_name, tags=["test", "integration-test"]
    )
    corr_flow.progress(result["run_id"], lambda x: print(f"Status: {x}"))
    status = corr_flow.get_status(result["run_id"])
    assert status["status"] == "SUCCEEDED"
