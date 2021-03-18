import os
import sys
from pprint import pprint
from pilot.client import PilotClient

from XPCS.tools.xpcs_metadata import gather

pc = PilotClient()
pc.project.current = 'xpcs-8id'

print('This script requires: '
      'https://github.com/globusonline/pilot1-tools/pull/125')


def get_diff(old, new):
    return [f"{k}: {old.get(k)} --> {new.get(k)}"
            for k in new.keys() if old.get(k) != new.get(k)]


def update_xpcs(dry_run=True):
    new_entries = {}
    for entry in pc.list_entries():
        files = entry['content'][0]['files']
        hdf = next(filter(lambda f: f['filename'].endswith('hdf'), files))
        short_path = pc.get_short_path(entry['subject'])
        if not os.path.exists(hdf['filename']):
            print(f'Downloading {hdf["filename"]}...')
            try:
                sp_url = pc.get_short_path(hdf['url'])
                pc.download(sp_url)
            except Exception as exc:
                print(f'Could not find {sp_url}! Skipping...')
                continue
        hdfinfo = gather(hdf['filename'])
        reg = pc.register(hdf['filename'], short_path, metadata=hdfinfo,
                          update=True, dry_run=True, skip_analysis=True)
        new_entries[short_path] = reg['new_metadata']
        if dry_run:
            prune = {'creators', 'publicationYear', 'publisher',
                     'resourceType', 'subjects'}
            hdfinfo = {k: v for k, v in hdfinfo.items() if k not in prune}
            hdfinfo['project-slug'] = pc.project.current
            old_meta = entry['content'][0]['project_metadata']
            old, new = set(old_meta), set(hdfinfo)
            print(f"REMOVED ITEMS -- {hdf['filename']}")
            pprint(old.difference(new) or 'Nothing removed')
            print(f"ADDED ITEMS -- {hdf['filename']}")
            pprint(new.difference(old) or 'Up to date')
            print(f"CHANGES -- {hdf['filename']}")
            pprint(get_diff(old_meta, hdfinfo))

    print('Validating and applying updates...')
    pc.ingest_many(new_entries, dry_run=dry_run)
    dr = '(Dry Run -- no updates made)' if dry_run else ''
    print(f'Updated {len(new_entries)} entries. {dr}')

if __name__ == '__main__':
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == '--no-dry-run':
        dry_run = False
    if dry_run:
        print('Doing dry run! To update for real, run with --no-dry-run')
    update_xpcs(dry_run)
