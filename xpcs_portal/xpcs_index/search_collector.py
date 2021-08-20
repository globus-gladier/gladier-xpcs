import logging
import os
import pathlib
import urllib
import collections
from concierge_app.search import SearchCollector

log = logging.getLogger(__name__)

XPCS_DATA_EXTENSIONS = ('.hdf', '.imm', '.bin')


def filter_xpcs_data_files(manifest, allowed_extensions=XPCS_DATA_EXTENSIONS):
    return [m for m in manifest if any(
        m['filename'].endswith(extension) for extension in allowed_extensions
    )]


class XPCSSearchCollector(SearchCollector):

    def get_manifest(self):
        data_manifest = []
        manifest = filter_xpcs_data_files(super().get_manifest())
        for man in filter_xpcs_data_files(super().get_manifest()):
            # Prefixing the filename with the hdf directory will cause manifests
            # to create a 'dataset' directory within the 'processing' directory,
            # where the .hdf and .imm/.bin will be kept separate from other
            # datasets

            man['filename'] = os.path.join(
                os.path.basename(os.path.dirname(man['url'])),
                os.path.basename(man['url'])
            )
            data_manifest.append(man)
        # log.debug(f'Collected {len(data_manifest)}/{len(manifest)} XPCS Data '
        #           f'files for reprocessing...')
        return data_manifest[0:100]


class XPCSReprocessingSearchCollector(SearchCollector):

    def get_manifest(self):
        """
        Organize the datasets into pairs, such that each .hdf and .imm/.bin file
        are together in the same list.
        :return: A list of smaller two-item lists
        """
        datasets = collections.defaultdict(list)
        for m in filter_xpcs_data_files(super().get_manifest()):
            # The directory will be the same for the .hdf and .imm/.bin pairs
            # Use the parent directory as the key to bind them together
            path = pathlib.Path(urllib.parse.urlparse(m['url']).path)
            datasets[path.parent.name].append(path)
        return list(datasets.values())
