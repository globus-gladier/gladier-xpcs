import sys
import pathlib
from gladier import GladierBaseTool, generate_flow_definition


def make_corr_plots(**data):
    import os
    import json
    from xpcs_webplot.plot_images import hdf2web_safe
    from xpcs_webplot import __version__ as webplot_version

    hdf2web_safe(data['hdf_file'], target_dir=data['proc_dir'], image_only=True)

    metadata = {
        'plotting': {
            'name': 'xpcs_webplot',
            'tool_version': str(webplot_version),
            'source': 'https://github.com/AZjk/xpcs_webplot',
        }
    }

    if data.get('plotting_metadata_file'):
        with open(data['plotting_metadata_file'], 'w') as f:
            f.write(json.dumps(metadata, indent=2))

    return [img for img in os.listdir(data['proc_dir']) if img.endswith('.png')]


@generate_flow_definition(modifiers={
    'make_corr_plots': {'WaitTime': 28800}
})
class MakeCorrPlots(GladierBaseTool):
    required_input = [
        'proc_dir',
        'hdf_file',
    ]

    funcx_functions = [
        make_corr_plots
    ]


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: python plot.py my_file.hdf')
    input_file = pathlib.Path(sys.argv[1]).absolute()
    # Create the 'expected' processing directory, which should look like this:
    # * Top level proc_dir/
    #     * HDF_Folder/
    #         * HDF_File.hdf
    make_corr_plots(proc_dir=str(input_file.parent.parent),
                    hdf_file=str(input_file))
