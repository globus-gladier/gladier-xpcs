import logging
import pathlib
import urllib
import collections
from concierge_app.search import SearchCollector

log = logging.getLogger(__name__)

ROOT_EXPECTED_PATH = '/XPCSDATA/Automate/'
XPCS_DATA_EXTENSIONS = ('.hdf', '.imm', '.bin', '.h5')


class XPCSReprocessingSearchCollector(SearchCollector):

    @staticmethod
    def filter_xpcs_data_files(manifest, allowed_extensions=XPCS_DATA_EXTENSIONS):
        data_files = [m for m in manifest 
                    if pathlib.Path(m['url']).suffix in allowed_extensions]
        return data_files

    def get_manifest(self):
        """
        Organize the datasets into pairs, such that each .hdf and .imm/.bin file
        are together in the same list.
        :return: A list of smaller two-item lists
        """
        log.info('Fetching manifest!')
        datasets = collections.defaultdict(list)
        for m in self.filter_xpcs_data_files(super().get_manifest()):
            # The directory will be the same for the .hdf and .imm/.bin pairs
            # Use the parent directory as the key to bind them together
            try:
                path = pathlib.Path(urllib.parse.urlparse(m['url']).path)
                dataset_path = path.relative_to(ROOT_EXPECTED_PATH)
                if dataset_path.parents[0] == '.':
                    raise ValueError(f'File found in root dataset directory: {m["url"]}')
                dataset_dir = dataset_path.parts[0]
                # log.debug(f'Adding parent {dataset_dir} for {path}')
                datasets[dataset_dir].append(path)
            except ValueError:
                log.debug('Failed to parse file', exc_info=True)
        return list(datasets.values())
