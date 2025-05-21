import sys
import pprint
import logging
import pathlib
from gladier import GladierBaseTool, generate_flow_definition


def make_corr_plots(
    corr_results: str,
    webplot_target_dir: str,
    webplot_extra_metadata=None,
    **data: dict,
):
    import os
    import json
    import time
    import pathlib
    from datetime import datetime
    from xpcs_webplot.plot_images import hdf2web_safe, XF, NpEncoder
    from xpcs_webplot import __version__ as webplot_version

    corr_start = time.time()
    webplot_output = hdf2web_safe(
        corr_results, target_dir=webplot_target_dir, image_only=True, overwrite=True
    )
    execution_time_seconds = round(time.time() - corr_start, 2)

    if webplot_output is None:
        raise Exception(
            "Plots failed to generate This is likely a problem with a bad input HDF "
            f"({corr_results})"
        )

    xf = XF(corr_results)
    metadata = xf.get_hdf_info()
    metadata["analysis_type"] = xf.atype
    metadata["start_time"] = xf.start_time
    metadata["plot_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tools = webplot_extra_metadata.get('tools', [])
    tools += [
        {
            "name": "xpcs_webplot",
            "tool_version": str(webplot_version),
            "execution_time_seconds": execution_time_seconds,
            "device": "cpu",
            "source": "https://github.com/AZjk/xpcs_webplot",
        }
    ]
    metadata["tools"] = tools
    metadata.update(webplot_extra_metadata or {})
    save_dir = (
        pathlib.Path(webplot_target_dir)
        / f"{pathlib.Path(corr_results).stem}"
        / "metadata.json"
    )
    with open(save_dir, "w") as f:
        json.dump({"project_metadata": metadata}, f, indent=4, cls=NpEncoder)

    return {
        "images": [
            img for img in os.listdir(webplot_target_dir) if img.endswith(".png")
        ],
        "images_directory": str(webplot_target_dir),
        "input_hdf": corr_results,
        "webplot_output": webplot_output,
        "execution_time_seconds": execution_time_seconds,
    }


@generate_flow_definition(
    modifiers={
        "make_corr_plots": {"WaitTime": 604800, "ExceptionOnActionFailure": True}
    }
)
class MakeCorrPlots(GladierBaseTool):
    required_input = [
        "corr_results",
        "webplot_target_dir",
    ]

    compute_functions = [make_corr_plots]


if __name__ == "__main__":
    # This will pick up logging in the webplot tool
    logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
    if len(sys.argv) != 2:
        print("Usage: python plot.py my_file.hdf")
    input_file = pathlib.Path(sys.argv[1]).absolute()
    # Create the 'expected' processing directory, which should look like this:
    # * Top level webplot_target_dir/
    #     * HDF_Folder/
    #         * HDF_File.hdf
    hdf_file = str(input_file)
    webplot_target_dir = str(
        input_file.parent / "output" / input_file.stem / "resources"
    )
    print(f"Plotting {hdf_file} at location {webplot_target_dir}")
    output = make_corr_plots(
        hdf_file=hdf_file,
        webplot_target_dir=webplot_target_dir,
    )
    pprint.pprint(output)
