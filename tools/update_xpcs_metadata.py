import sys
from pprint import pprint
from pilot.client import PilotClient

pc = PilotClient()
pc.project.current = 'xpcs-8id'

print('Please ensure you run this with the PR here: \n'
      'pip install git+https://github.com/globusonline/pilot1-tools@refs/pull/127/head#egg=pilot1-tools')  # noqa


def get_aps_key(meta):
    # Delete the old one
    meta['project_metadata'].pop('aps_cycle', None)
    root_key = 'measurement.instrument.acquisition.root_folder'
    aps_cycle_key = 'aps_cycle_v2'
    root = meta['project_metadata'][root_key].lstrip('/').rstrip('/')
    _, aps_cycle, user_str = root.split('/')
    meta['project_metadata'][aps_cycle_key] = '/'.join((aps_cycle, user_str))
    return meta


def update_xpcs(dry_run=True, n=0):
    new_entries = {}
    entries = pc.list_entries()
    entries = entries if n == 0 else entries[0:n]
    for entry in entries:
        meta = entry['content'][0]
        new_meta = get_aps_key(meta)
        new_entries[pc.get_short_path(entry['subject'])] = new_meta
    # print('Validating and applying updates...')
    results = pc.ingest_many(new_entries, dry_run=dry_run, relative=True)
    return results


if __name__ == '__main__':
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1] == '--no-dry-run':
        dry_run = False
    if dry_run:
        print('Doing dry run! To update for real, run with --no-dry-run')
        if len(sys.argv) > 1 and sys.argv[1] == '--all':
            results = update_xpcs(dry_run)
            print('Processing on all records was successful')
        else:
            results = update_xpcs(dry_run, n=1)
            pprint(results)
            print('Set --all to run on all records.')
    else:
        results = update_xpcs(dry_run)
    dr = '(Dry Run -- no updates made)' if dry_run else ''
    print(f'Updated {len(results["ingest_data"]["gmeta"])} entries. {dr}')
