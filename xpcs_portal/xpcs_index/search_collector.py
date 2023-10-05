import logging
import pathlib
import urllib
import copy
import collections
from xpcs_portal.xpcs_index.search import SearchCollector
from gladier_xpcs.flows.flow_boost import XPCSBoost

log = logging.getLogger(__name__)


class XPCSReprocessingSearchCollector(SearchCollector):


    DEFAULT_SEARCH_KWARGS = {
        'limit': 1,
        'filters': [],
    }
    XPCS_DATA_EXTENSIONS = ('.hdf', '.imm', '.bin')

    def filter_xpcs_data_files(manifest, allowed_extensions=XPCS_DATA_EXTENSIONS):
        return [m for m in manifest if any(
            m['filename'].endswith(extension) for extension in allowed_extensions
        )]

    @classmethod
    def get_dataset_name(cls, manifest):
        """XPCS Dataset names are based on the initial HDF file passed in, without the extension"""
        return pathlib.Path(cls.get_single_file_with_extension(manifest, ['.hdf'])["filename"]).stem

    @classmethod
    def get_dataset_directory(cls, manifest) -> pathlib.Path:
        hdf = pathlib.Path(cls.get_hdf(manifest))
        dataset_name = cls.get_dataset_name(manifest)

        index = list(reversed(hdf.parts)).index(dataset_name)
        path = pathlib.Path(str(hdf))
        for _ in range(index):
            path = path.parent
        return path

    @classmethod
    def get_single_file_with_extension(cls, manifest, extensions: list):
        files = cls.filter_xpcs_data_files(manifest, extensions)
        filenames = {f['filename'] for f in files}
        if len(filenames) != 1:
            raise ValueError('Multiple filenames')
        return files[0]

    @classmethod
    def get_hdf(cls, manifest):
        return cls.get_manifest_url_path(cls.get_single_file_with_extension(manifest, ['.hdf'])['url'])

    
    @classmethod
    def get_imm(cls, manifest):
        return cls.get_manifest_url_path(cls.get_single_file_with_extension(manifest, ['.imm'])['url'])


    @staticmethod
    def get_manifest_url_path(url: str) -> pathlib.Path:
        return pathlib.Path(urllib.parse.urlparse(url).path)

    def get_input_files(self, deployment, data):
        """
        Organize the datasets into pairs, such that each .hdf and .imm/.bin file
        are together in the same list.
        :return: A list of smaller two-item lists
        """
        datasets = list()
        for dataset in self.get_manifest():
            flow_input = self.get_xpcs_input(
                deployment,
                self.get_dataset_directory(dataset),
                self.get_hdf(dataset),
                self.get_imm(dataset),
                pathlib.Path(data['qmap_parameter_file']),
            )
            datasets.append(flow_input)
        return datasets

    @staticmethod
    def get_xpcs_input(deployment, dataset_dir, hdf_source, imm_source, qmap_source):
        flow_input = XPCSBoost(login_manager=None).get_xpcs_input(deployment, str(imm_source), str(hdf_source), str(qmap_source))
        flow_input['input'].update(deployment.function_ids)
        return flow_input
