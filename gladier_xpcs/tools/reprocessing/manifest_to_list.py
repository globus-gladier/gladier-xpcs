from gladier import GladierBaseTool, generate_flow_definition


def manifest_to_list(data):
    """Create funcx execution tasks given a manifest"""
    import os
    import json
    import requests
    from urllib.parse import urlparse

    url = (f'https://develop.concierge.nick.globuscs.info/api/manifest/'
           f'{data["manifest_id"]}/remote_file_manifest/')
    response = requests.get(url).json()
    paths = [m['url'] for m in response['remote_file_manifest']]

    # Force all corr related files
    data['manifest_to_funcx_tasks_suffixes'] = ['hdf', 'imm', 'bin']
    if data.get('manifest_to_funcx_tasks_suffixes'):
        paths = [p for p in paths
                 if any((p.endswith(s) for s in data['manifest_to_funcx_tasks_suffixes']))]

    # Chop paths prefix
    paths = [os.path.join(os.path.basename(os.path.dirname(p)), os.path.basename(p))
             for p in paths]
    dataset_paths = {os.path.dirname(p) for p in paths}
    task_payloads = []
    for dp in dataset_paths:
        proc_dir = os.path.join(urlparse(data['manifest_destination']).path, dp)

        pl_data = [p for p in paths if p.startswith(dp)]
        hdfs = [os.path.join(proc_dir, os.path.basename(p))
                for p in pl_data if p.endswith('.hdf')]
        hdfs = [h for h in hdfs if os.path.exists(h)]
        if not hdfs:
            continue
        # Purposely don't raise. This will cause corr to fail with data on which task was bad
        hdf = hdfs[0]
        imms = [os.path.join(proc_dir, os.path.basename(p))
               for p in pl_data if p.endswith('.imm') or p.endswith('.bin')]
        if not imms:
            continue
        imm = imms[0]

        # There's a nasty assumption we always make with datasets where the dirname will match
        # the filename. This isn't always the case with reprocessing, so this ugly block will
        # enforce that.
        try:
            hdf_fname, hdf_ext = os.path.splitext(os.path.basename(hdf))
            proc_dir_fname = os.path.basename(proc_dir)
            if hdf_fname != proc_dir_fname:
                new_hdf_fname = os.path.join(proc_dir, f'{proc_dir_fname}{hdf_ext}')
                if os.path.exists(new_hdf_fname):
                    os.unlink(new_hdf_fname)
                os.rename(hdf, new_hdf_fname)
                hdf = new_hdf_fname

            if not os.path.exists(hdf):
                raise Exception(f'No hdf file: {hdf}')
            if not os.path.exists(imm):
                raise Exception(f'No imm file: {imm}')
        except Exception as e:
            with open(os.path.join(proc_dir, 'path_errors.log'), 'w+') as f:
                f.write(str(e))
            continue

        payload = {
            'proc_dir': proc_dir,
            'corr_loc': data['corr_loc'],
            'reprocessing_suffix': data['reprocessing_suffix'],
            'hdf_file': os.path.join(proc_dir, os.path.basename(hdf)),
            'imm_file': os.path.join(proc_dir, os.path.basename(imm)),
            'qmap_file': os.path.join(proc_dir, data['qmap_file']),
            'parameter_file': os.path.join(proc_dir, 'parameters.json')
        }
        with open(payload['parameter_file'], 'w+') as f:
            f.write(json.dumps(payload, indent=4))
        task_payloads.append({'parameter_file': payload['parameter_file']})
    return task_payloads


@generate_flow_definition(modifiers={
    manifest_to_list: {'endpoint': 'funcx_endpoint_non_compute'},
})
class ManifestToList(GladierBaseTool):

    flow_input = {}

    required_input = [
        'manifest_id',
    ]

    funcx_functions = [manifest_to_list]
