import os
import sys
import globus_sdk
from fair_research_login import NativeClient
import re
try:
    from tqdm import tqdm
except ImportError:
    print('Do "pip install tqdm" for fancy progress bars.')
    tqdm = list  # If not installed, do nothing to iterables

CLIENT_ID = 'd9366faf-42a5-4840-b3f5-95711f64bf36'
CLUTCH = 'b0e921df-6d04-11e5-ba46-22000b92c6ec'


def get_transfer_client():
    nc = NativeClient(app_name='Clutch Crawler', client_id=CLIENT_ID)
    nc.login()
    return globus_sdk.TransferClient(authorizer=nc.get_authorizers()['transfer.api.globus.org'])


def get_dir(path, ep=CLUTCH):
    tc = get_transfer_client()
    return [e['name'] for e in tc.operation_ls(ep, path=path) if e['type'] == 'dir']


def search_dir(path, pattern, ep=CLUTCH):
    return [d for d in get_dir(path, ep) if re.match(pattern, d)]


def get_datasets(paths, ep=CLUTCH):
    """Given a list of paths on a remote Globus Endpoint, fetch a list of valid
    datasets we can run through our XPCS flow."""
    tc = get_transfer_client()
    data = dict(valid=[], invalid=[], valid_datasets=[])
    try:
        for path in tqdm(paths):
            gpath = tc.operation_ls(ep, path=path)
            hdf = list(filter(lambda p: p['name'].endswith('.hdf'), gpath))
            imm = list(filter(lambda p: p['name'].endswith('.imm'), gpath))
            if len(hdf) == 1 and len(imm) == 1:
                data['valid'].append(path)
                data['valid_datasets'].append(
                    (os.path.join(path, hdf[0]['name']),
                     os.path.join(path, imm[0]['name']))
                )
            else:
                data['invalid'].append(path)
    except KeyboardInterrupt:
        print('Aborted!')
        sys.exit(-1)
    return data