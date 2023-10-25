import logging
import pathlib
import urllib
import copy
import collections
from globus_app_flows.collectors.search import SearchCollector
from gladier_xpcs.deployments import deployment_map
from gladier_xpcs.flows.flow_boost import XPCSBoost

log = logging.getLogger(__name__)


class XPCSSearchCollector(SearchCollector):

    import_string = 'xpcs_portal.xpcs_index.views.XPCSSearchCollector'

    @staticmethod
    def parse_url(url):
        return pathlib.Path(urllib.parse.urlparse(url).path)

    @classmethod
    def get_files_based_on_parent(cls, files, parent):
        input_files = []
        for f in files:
            path = pathlib.Path(cls.parse_url(f['url']))
            if path.parent.name == 'input':
                input_files.append(path)
        return input_files

    @staticmethod
    def get_file_by_extension(files: list, extension: str):
        for f in files:
            if pathlib.Path(f).suffix == extension:
                return str(f)
        raise ValueError(f'Could not find extension "{extension}" in files: {files}')

    @staticmethod
    def get_dataset_directory(hdf_file: str) -> pathlib.Path:
        dataset_name = pathlib.Path(hdf).name

        index = list(reversed(hdf.parts)).index(dataset_name)
        path = pathlib.Path(str(hdf))
        for _ in range(index):
            path = path.parent
        return path 

    def get_run_input(self, collector_data, form_data):
        files = collector_data['entries'][0]['content']['files']
        input_files = self.get_files_based_on_parent(files, 'input')
        hdf_file = self.get_file_by_extension(input_files, '.hdf')
        imm_file = self.get_file_by_extension(input_files, '.imm')
        deployment = deployment_map[form_data['facility']]

        run_input = XPCSBoost(login_manager=None).get_xpcs_input(deployment, imm_file, hdf_file, form_data['qmap_parameter_file'])
        run_input['input'].update(deployment.function_ids)
        return run_input

    def get_run_start_kwargs(self, collector_data, form_data):
        files = collector_data['entries'][0]['content']['files']
        input_files = self.get_files_based_on_parent(files, 'input')
        hdf_file = self.get_file_by_extension(input_files, '.hdf')
        return {
            "label": pathlib.Path(hdf_file).name,
            "tags": ["DGA Testing", "XPCS", "APS"],
            "run_monitors": ["urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"],
            "run_managers": ["urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"],
        }
