
def xpcs_corr(event):
    import os
    import subprocess
    from subprocess import PIPE

    print("Starting XPCS Corr")

    hdf_file = event['data']['hdf']
    imm_file = event['data']['imm']
    dirname = os.path.dirname(hdf_file)

    flags = ""
    if "flags" in event['data']:
        flags = event['data']

    cmd = f"/soft/datascience/xpcs_eigen/build/corr {hdf_file} -imm {imm_file} {flags}"
    cmd = cmd.strip().split(" ")

    res = subprocess.run(cmd, stdout=PIPE, stderr=PIPE)
    def write_log(fname, data):
        if data:
            with open(os.path.join(dirname, fname), 'w+') as f:
                f.write(data)
    write_log('corr_output.log', res.stdout.decode('utf-8'))
    write_log('corr_errors.log', res.stderr.decode('utf-8'))
    return str(res.stdout)


def xpcs_plot(event):
    import os
    from XPCS.tools import xpcs_plots

    dir_name = os.path.dirname(event['data']['hdf'])

    os.chdir(dir_name)
    xpcs_plots.make_plots(event['data']['hdf'])
    return [img for img in os.listdir('.') if img.endswith('.png')]


def xpcs_pilot(event):
    import os
    import json
    import shutil
    import datetime
    from XPCS.tools.xpcs_metadata import gather
    from pilot.client import PilotClient
    hdf_file = event['data']['hdf']
    exp_dir = os.path.dirname(hdf_file)
    exp_name = os.path.basename(hdf_file).replace(".hdf", "")
    metadata = gather(hdf_file)
    metadata.update({
            'description': f'{exp_name}: Automated data processing.',
            'creators': [{'creatorName': '8-ID'}],
            'publisher': 'Automate',
            'title': exp_name,
            'subjects': [{'subject': 'XPCS'}, {'subject': '8-ID'}],
            'publicationYear': f'{datetime.datetime.now().year}',
            'resourceType': {
                'resourceType': 'Dataset',
                'resourceTypeGeneral': 'Dataset'
            }
        })
    extra_metadata = event['data'].get('metadata', {}) or {}
    metadata.update(extra_metadata)
    # Create metadata file
    # Some types have changed between search ingests, and they cause the search ingest
    # to fail. Pop them so we don't get the search error.
    for evil_key in ['exchange.partition_norm_factor']:
        if evil_key in metadata.keys():
            metadata.pop(evil_key)

    # ONLY upload images, this is required for using the test server
    # We can remove this when petrel is back up and running
    upload_dir = os.path.join(exp_dir, exp_name)
    if not os.path.exists(upload_dir):
        os.mkdir(upload_dir)
    for image in [img for img in os.listdir(exp_dir) if img.endswith('.png')]:
        shutil.copy(os.path.join(exp_dir, image), os.path.join(upload_dir, image))

    pc = PilotClient()
    assert pc.context.current == 'xpcs', 'Pilot Context is not XPCS!'
    pc.project.current = 'xpcs-8id'
    try:
        result = pc.upload(upload_dir, '/', metadata=metadata, dry_run=False, update=True, skip_analysis=True)
        # Remove some large keys from result, upload everything else
        # return {k: v for k, v in result.items()
        #         if k not in ['new_metadata', 'previous_metadata', 'upload']}
        return f'Upload Successful: {os.path.basename(upload_dir)}'
    except Exception as e:
        metadata_f = os.path.join(upload_dir, 'metadata.json')
        with open(metadata_f, 'w+') as f:
            f.write(json.dumps(metadata, indent=2))
        # return {
        #     'error': f'Failed to upload to search, likely a broken key. Metadata dumped.',
        #     'metadata_filename': metadata_f,
        #     'exception': str(e),
        # }
        return f'Upload Failed: {os.path.basename(upload_dir)}'
