import logging
import pathlib
import urllib
import copy
import collections
from globus_app_flows.collectors.search import SearchCollector
from globus_app_flows.collectors.transfer import TransferCollector
from gladier_xpcs.deployments import deployment_map
from gladier_xpcs.flows.flow_boost import XPCSBoost

log = logging.getLogger(__name__)


class XPCSTransferCollector(TransferCollector):

    import_string = "xpcs_portal.xpcs_index.collectors.XPCSTransferCollector"
    SKIP_FOLDERS = ["ALCF_results", "cluster_results", "logs"]

    def __init__(self, *args, **kwargs):
        self.cluster_results = None
        # super().__init__(collection="74defd5b-5f61-42fc-bcc4-834c9f376a4f", path="/XPCSDATA/2019-1/comm201901/", user=kwargs["user"])
        super().__init__(*args, **kwargs)

    def get_files(self, globus_dir: str):
        tc = self.get_transfer_client()
        response = tc.operation_ls(self.collection, path=globus_dir)
        return [
            str(pathlib.Path(globus_dir) / f["name"])
            for f in response.data["DATA"]
            if f["type"] == "file"
        ]

    def get_cluster_results(self, hdf_file: str):
        if self.cluster_results:
            log.debug(f"Fetching CACHED cluster results")
            return self.cluster_results

        cluster_dir = pathlib.Path(hdf_file).parent.parent / "cluster_results"
        self.cluster_results = self.get_files(cluster_dir)
        return self.cluster_results

    def get_qmap_file(self, hdf_file: str):
        for f in self.get_cluster_results(hdf_file):
            if pathlib.Path(hdf_file).name == pathlib.Path(f).name:
                return f

    def get_run_input(self, collector_data, form_data):
        """
        This is the main interesting method. Collect all files for running this XPCS dataset.
        """

        dataset = pathlib.Path(self.path) / collector_data["name"]
        files = self.get_files(dataset)

        hdf_file = XPCSSearchCollector.get_file_by_extension(files, ".hdf")
        imm_file = XPCSSearchCollector.get_file_by_extension(files, ".imm")
        qmap_file = self.get_qmap_file(hdf_file)
        deployment = deployment_map[form_data["facility"]]

        run_input = XPCSBoost(login_manager=None).get_xpcs_input(
            deployment, imm_file, hdf_file, qmap_file
        )
        run_input["input"].update(deployment.function_ids)
        return run_input

    def get_run_start_kwargs(self, collector_data, form_data):
        return {
            "label": pathlib.Path(collector_data["name"]).name,
            "tags": ["DGA Testing", "XPCS", "APS"],
            "run_monitors": [
                "urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"
            ],
            "run_managers": [
                "urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"
            ],
        }


class XPCSSearchCollector(SearchCollector):

    import_string = "xpcs_portal.xpcs_index.collectors.XPCSSearchCollector"

    @staticmethod
    def parse_url(url):
        return pathlib.Path(urllib.parse.urlparse(url).path)

    @classmethod
    def get_files_based_on_parent(cls, files, parent):
        input_files = []
        for f in files:
            path = pathlib.Path(cls.parse_url(f["url"]))
            if path.parent.name == "input":
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
        hdf_file_path = pathlib.Path(hdf_file)
        index = list(reversed(hdf_file_path.parts)).index(hdf_file_path.name)

        path = hdf_file_path
        for _ in range(index):
            path = path.parent
        return path

    def get_run_input(self, collector_data, form_data):
        files = collector_data["entries"][0]["content"]["files"]
        input_files = self.get_files_based_on_parent(files, "input")
        hdf_file = self.get_file_by_extension(input_files, ".hdf")
        imm_file = self.get_file_by_extension(input_files, ".imm")
        deployment = deployment_map[form_data["facility"]]

        run_input = self.generate_run_input(deployment, imm_file, hdf_file, form_data["qmap_parameter_file"])
        run_input["input"].update(deployment.function_ids)
        return run_input

    def generate_run_input(self, deployment, imm_file, hdf_file, qmap_file) -> dict:
        gpu_flag = 0
        if deployment.service_account is False:
            gpu_flag = -1
        log.info(f"GPU Flag set to {gpu_flag} for service account {deployment.__class__.__name__}")
        return XPCSBoost(login_manager=None).get_xpcs_input(
            deployment, imm_file, hdf_file, qmap_file, gpu_flag=gpu_flag
        )

    def get_run_start_kwargs(self, collector_data, form_data):
        files = collector_data["entries"][0]["content"]["files"]
        input_files = self.get_files_based_on_parent(files, "input")
        hdf_file = self.get_file_by_extension(input_files, ".hdf")
        return {
            "label": pathlib.Path(hdf_file).name,
            "tags": ["DGA Testing", "XPCS", "APS"],
            "run_monitors": [
                "urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"
            ],
            "run_managers": [
                "urn:globus:groups:id:368beb47-c9c5-11e9-b455-0efb3ba9a670"
            ],
        }

class XPCSSuffixSearchCollector(XPCSSearchCollector):
    """
    This is for demoing stuff at SC. This should probably be removed at some point.

    Basically, re-run a record and ingest it with a new suffix
    """

    import_string = "xpcs_portal.xpcs_index.collectors.XPCSSuffixSearchCollector"

    def get_run_input(self, collector_data, form_data):
        files = collector_data["entries"][0]["content"]["files"]
        input_files = self.get_files_based_on_parent(files, "input")
        hdf_file = self.get_file_by_extension(input_files, ".hdf")
        imm_file = self.get_file_by_extension(input_files, ".imm")

        dataset_dir = pathlib.Path(hdf_file).stem
        new_dataset_dir = f"{dataset_dir}_new_suffix"

        deployment = deployment_map[form_data["facility"]]

        run_input = self.generate_run_input(deployment, imm_file.replace(dataset_dir, new_dataset_dir), hdf_file.replace(dataset_dir, new_dataset_dir), form_data["qmap_parameter_file"])
        # Replace the suffix name with the "old" name, which is the only thing that should be "normal"
        for item in run_input["input"]["source_transfer"]["transfer_items"]:
            item["source_path"] = item["source_path"].replace(new_dataset_dir, dataset_dir)

        run_input["input"].update(deployment.function_ids)
        return run_input