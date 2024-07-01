import pytest
from gladier_xpcs.flows.flow_reprocess import XPCSReprocessingFlow
from gladier_xpcs.reprocessing_tools.publish_preparation import publish_preparation


# This test is old, and no longer accounts for the way we currently run deployments
# via service accounts. We should revisit these and make them work, but there currently
# isn't time, and we will likely throw out this current way of doing things soon for a
# simpler client that will work better across beamlines.
@pytest.mark.skip
def test_reprocessing_get_xpcs_input(reprocessing_deployment, reprocessing_input):
    """Make sure input is being built correctly from deployments"""
    cli = XPCSReprocessingFlow()
    fi = cli.get_xpcs_input(
        reprocessing_deployment,
        reprocessing_input["hdf_source"],
        reprocessing_input["imm_source"],
        reprocessing_input["qmap_source_path"],
    )
    assert fi["input"]["hdf_file"] == (
        "/projects/APSDataAnalysis/xpcs/mock_staging_dir/"
        "A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_0001-1000.hdf"
    )
    assert fi["input"]["imm_file"] == (
        "/projects/APSDataAnalysis/xpcs/mock_staging_dir/"
        "A001_Aerogel_1mm_att6_Lq0_001_0001-1000/A001_Aerogel_1mm_att6_Lq0_001_00001-01000.imm"
    )


@pytest.mark.skip
def test_publish_preparation(mock_pathlib, reprocessing_runtime_input):
    from pprint import pprint

    pprint(reprocessing_runtime_input)
    result = publish_preparation(**reprocessing_runtime_input)
    assert result["hdf_file"] == (
        "/projects/APSDataAnalysis/xpcs/mock_staging_dir/"
        "A001_Aerogel_1mm_att6_Lq0_001_0001-1000<qmap_suffix>/A001_Aerogel_1mm_att6_Lq0_001_0001-1000<qmap_suffix>.hdf"
    )
    assert result["pilot"]["dataset"] == (
        "/projects/APSDataAnalysis/xpcs/mock_staging_dir/"
        "A001_Aerogel_1mm_att6_Lq0_001_0001-1000<qmap_suffix>"
    )
