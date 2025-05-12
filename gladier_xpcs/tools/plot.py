import sys
import pprint
import logging
import pathlib
from gladier import GladierBaseTool, generate_flow_definition


def make_corr_plots(hdf_file: str, webplot_target_dir: str, plotting_metadata_file: str = None, webplot_extra_metadata=None, **data: dict):
    import os
    import json
    import time
    import pathlib
    from xpcs_webplot.plot_images import hdf2web_safe
    from xpcs_webplot import __version__ as webplot_version

    corr_start = time.time()
    webplot_output = hdf2web_safe(hdf_file, target_dir=webplot_target_dir, image_only=True, overwrite=True)
    execution_time_seconds = round(time.time() - corr_start, 2)

    # Update the metadata file to point to "project metadata" inside the schema. Add some extra stuff there.
    with open(pathlib.Path(webplot_target_dir) / f"{pathlib.Path(hdf_file).stem}" / "metadata.json" , 'r+') as f:
        plot_metadata = {"project_metadata": json.loads(f.read())}
        if webplot_extra_metadata:
            plot_metadata["project_metadata"].update(webplot_extra_metadata.get("project_metadata", {}))
        f.seek(0)
        f.write(json.dumps(plot_metadata, indent=4))
        f.truncate()

    metadata = {
        'executable': {
            'name': 'xpcs_webplot',
            'tool_version': str(webplot_version),
            'execution_time_seconds': execution_time_seconds,
            'device': 'cpu',
            'source': 'https://github.com/AZjk/xpcs_webplot',
        }
    }

    if plotting_metadata_file:
        with open(plotting_metadata_file, 'w') as f:
            f.write(json.dumps(metadata, indent=2))

    if webplot_output is False:
        raise Exception(f"Plots failed to generate This is likely a problem with a bad input HDF "
                        f"({hdf_file}) or the target dir exists already {webplot_target_dir}")

    return {
        "images": [img for img in os.listdir(webplot_target_dir) if img.endswith('.png')],
        "images_directory": webplot_target_dir,
        "input_hdf": hdf_file,
        "webplot_output": webplot_output,
        "metadata": metadata,
    }


@generate_flow_definition(modifiers={
    'make_corr_plots': {'WaitTime': 28800}
})
class MakeCorrPlots(GladierBaseTool):
    required_input = [
        'webplot_target_dir',
        'hdf_file',
    ]

    compute_functions = [
        make_corr_plots
    ]


if __name__ == '__main__':
    # This will pick up logging in the webplot tool
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            logging.StreamHandler()
        ]
    )
    if len(sys.argv) != 2:
        print('Usage: python plot.py my_file.hdf')
    input_file = pathlib.Path(sys.argv[1]).absolute()
    # Create the 'expected' processing directory, which should look like this:
    # * Top level webplot_target_dir/
    #     * HDF_Folder/
    #         * HDF_File.hdf
    hdf_file = str(input_file)
    webplot_target_dir = str(input_file.parent / "output" / input_file.stem / "resources")
    print(f"Plotting {hdf_file} at location {webplot_target_dir}")
    output = make_corr_plots(hdf_file=hdf_file, webplot_target_dir=webplot_target_dir,)
    pprint.pprint(output)
