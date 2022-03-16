from gladier import GladierBaseTool, generate_flow_definition


def make_corr_plots(**data):
    import os
    import logging
    import json
    from xpcs_webplot.plot_images import hdf2web_safe
    from xpcs_webplot import __version__ as webplot_version


    log_file = os.path.join(data['proc_dir'], 'webplot.log')

    handlers = (
        # Useful for flows, logging will be captured in a file
        logging.FileHandler(filename=log_file),
        # Useful for testing, will only output when run directly on compute
        logging.StreamHandler(),
    )
    logging.basicConfig(handlers=handlers, level=logging.INFO)

    if not os.path.exists(data['proc_dir']):
        raise NameError(f'{data["proc_dir"]} \n Proc dir does not exist!')

    img_dir = os.path.join(data['proc_dir'], os.path.dirname(data['hdf_file']))
    os.chdir(img_dir)
    try:
        h5filename = os.path.join(data['proc_dir'], data['hdf_file'])
        hdf2web_safe(h5filename, target_dir=data['proc_dir'],
                     images_only=True)
    except (Exception, SystemExit) as e:
        return str(e)
    
    metadata = {
        'executable' : {
            'name': 'xpcs_webplot',
            'tool_version': str(webplot_version),
            'source': 'https://github.com/AZjk/xpcs_webplot',
            }
    }

    if data.get('execution_metadata_file'):
        with open(data['execution_metadata_file'], 'w') as f:
            f.write(json.dumps(metadata, indent=2))

    # logs = []
    # if os.path.exists(log_file):
    #     with open(log_file) as f:
    #         logs = f.readlines()

    # return {
    #     'result': 'SUCCESS',
    #     # 'boost_corr_log': logs,
    #     'proc_dir': data['proc_dir'],
    #     'xpcs_webplot': data['xpcs_webplot'],
    # }
    return [img for img in os.listdir(img_dir) if img.endswith('.png')]


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
