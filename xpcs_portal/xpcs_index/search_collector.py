import logging
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
