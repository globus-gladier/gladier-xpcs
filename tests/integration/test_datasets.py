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


def get_dataset_label(dataset):
    cpu = "CPU" if dataset.get("gpu_id") == -1 else ""
    return f"{dataset['dataset_type']} -- {dataset['type']} {dataset.get('size', '')} {cpu}"


DEPLOYMENT = "voyager-8idi-polaris"
DATASETS = [
    {
        "dataset_type": "Rigaku3m",
        "type": "Multitau",
        "raw": "/system/test/s8iddm_boost_corr_test/data/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001/01bsaxs12959_PA-pEG-ITO-A_a0208_f100000_r00001.bin.000",
        "qmap": "/system/test/s8iddm_boost_corr_test/data/rigaku_qmap_Sq360_Dq36_Lin_0501.hdf",
        "experiment": "integration-test202507",
        "cycle": "2025-2",
    },
    # Currently fails due to Twotime sizes being too large.
    # {
    #     "dataset_type": "Rigaku3m",
    #     "type": "Twotime",
    #     "gpu_id": -1,
    #     "size": "11.4 MB",
    #     "raw": "/gdata/dm/8IDI/2025-1/milliron202503/data/01fsaxs13072_toluene-blank_a0208_f100000_r00001/01fsaxs13072_toluene-blank_a0208_f100000_r00001.bin.000",
    #     "qmap": "/gdata/dm/8IDI/2025-1/milliron202503/data/rigaku_qmap_Sq360_Dq36_Lin_0501.hdf",
    #     "experiment": "integration-test202507",
    #     "cycle": "2025-2",
    # },
    {
        "dataset_type": "Eiger4m",
        "type": "Multitau",
        "raw": "/system/test/s8iddm_boost_corr_test/data/Da0194_P4-5wv-35C_a0008_f004000_r00001/Da0194_P4-5wv-35C_a0008_f004000_r00001.h5",
        "qmap": "/system/test/s8iddm_boost_corr_test/data/eiger4m_qmap_S360_D36_Lin.hdf",
        "experiment": "integration-test202507",
        "cycle": "2025-2",
    },
    {
        "dataset_type": "Eiger4m",
        "type": "Twotime",
        "size": "397 MB",
        "raw": "/system/test/s8iddm_boost_corr_test/data/A0195_P52_quiescent_a0005_f002000_r00001/A0195_P52_quiescent_a0005_f002000_r00001.h5",
        "qmap": "/system/test/s8iddm_boost_corr_test/data/eiger4m_qmap_default.hdf",
        "experiment": "integration-test202507",
        "cycle": "2025-2",
    },
    {
        "dataset_type": "Eiger4m",
        "type": "Twotime",
        "size": "404 MB",
        "gpu_id": -1,
        "raw": "/system/test/s8iddm_boost_corr_test/data/A0067_SIO2_P50_Load1_att_10_5_2_1_Quiescent_a0005_f002000_r00001/A0067_SIO2_P50_Load1_att_10_5_2_1_Quiescent_a0005_f002000_r00001.h5",
        "qmap": "/system/test/s8iddm_boost_corr_test/data/eiger4m_qmap_default.hdf",
        "experiment": "integration-test202507",
        "cycle": "2025-2",
    },
    # NOTE! This dataset is pretty large and so is pretty slow. Not recommended unless you really want to.
    # {
    #     "dataset_type": "Eiger4m",
    #     "type": "Twotime",
    #     "size": "1.4 GB",
    #     # Failed on GPU. Error: Tried to allocate 132.01 GiB. GPU 0 has a total capacity of 39.39 GiB of which 38.91 GiB is free.
    #     # When run on CPU, this runs fine.
    #     "gpu_id": -1,
    #     "raw": "/system/test/s8iddm_boost_corr_test/data/A0160_G48_attenuation_test_a0027_f010000_r00001/A0160_G48_attenuation_test_a0027_f010000_r00001.h5",
    #     "qmap": "/system/test/s8iddm_boost_corr_test/data/eiger4m_qmap_default.hdf",
    #     "experiment": "integration-test202507",
    #     "cycle": "2025-2",
    # },
]


@pytest.fixture
def boost_corr_arguments():
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
        # "gpu_id": 0,
        # "output": "",
        "overwrite": True,
        # "qmap": "",
        # "raw": "",
        "save_g2": False,
        "smooth": "sqmap",
        "stride_frame": 1,
        # "type": "",
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
            "type": dataset["type"],
            # By default use GPU. -1 for CPU
            "gpu_id": dataset.get("gpu_id", 0),
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
    "dataset", DATASETS, ids=lambda d: get_dataset_label(d)
)
def test_datasets(dataset, boost_corr_arguments):

    flow_input = _get_flow_input(dataset, boost_corr_arguments)

    run_kwargs = {"run_monitors": flow_input["input"]["publishv2"]["visible_to"]}
    flows_manager = FlowsManager(run_kwargs=run_kwargs)

    corr_flow = XPCSBoost(flows_manager=flows_manager)

    assert corr_flow.get_flow_id()
    assert "xpcs_boost_corr_function_id" in corr_flow.get_input()["input"]
    result = corr_flow.run_flow(
        flow_input=flow_input, label=get_dataset_label(dataset), tags=["test", "integration-test"]
    )
    corr_flow.progress(result["run_id"], lambda x: print(f"Status: {x}"))
    status = corr_flow.get_status(result["run_id"])
    assert status["status"] == "SUCCEEDED"
