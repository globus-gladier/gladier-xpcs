import json
import pathlib
import logging
from django import forms
from xpcs_portal.xpcs_index import models
from concierge_app.forms import SubjectSelectManifestCheckoutForm

from xpcs_portal.xpcs_index.search_collector import XPCSReprocessingSearchCollector
from xpcs_portal.xpcs_index.exc import DataError

log = logging.getLogger(__name__)


class ReprocessDatasetsCheckoutForm(SubjectSelectManifestCheckoutForm):
    SEARCH_COLLECTOR_CLASS = XPCSReprocessingSearchCollector

    options_cache = forms.CharField(label='Options', required=False)
    qmap_ep = forms.CharField(required=False)
    qmap_path = forms.CharField(required=False)
    reprocessing_suffix = forms.CharField(required=False)

    class Meta:
        model = models.ReprocessingTask
        fields = ['query', 'options_cache', 'qmap_ep', 'qmap_path', 'reprocessing_suffix']
    
    def clean(self):
        cleaned_data = super().clean()
        sc = self.get_search_collector()
        datasets = list()
        for dataset in sc.get_manifest():
            try:
                datasets.append({
                    'raw_data': str(self.get_raw_data(dataset)),
                    'hdf_data': str(self.get_hdf_data(dataset)),
                    'qmap_file': str(self.get_qmap_file(dataset)),
                })
            except DataError:
                log.debug(f'Failed to collect datasets for one')
        cleaned_data['datasets'] = datasets
        return cleaned_data

    def get_raw_data(self, dataset):
        for f in dataset:
            filename = pathlib.Path(f)
            if filename.parent.name == 'input' and filename.suffix in ('.imm', '.bin'):
                return filename
        raise DataError('Unable to find Raw Data input (imm, bin, hdf, etc)')
    
    def get_hdf_data(self, dataset):
        for f in dataset:
            filename = pathlib.Path(f)
            log.debug(filename)
            if filename.parent.name == 'input' and filename.suffix == '.hdf':
                return filename
        raise DataError('Unable to find HDF data input (imm, bin, hdf, etc)')

    def get_qmap_file(self, dataset):
        for f in dataset:
            if pathlib.Path(f).parent.name == 'qmap':
                return f
        raise DataError('Unable to find Qmap file')


    def get_search_collector(self):
        log.debug('Fetching search collector (For some reason this step takes a while)!')
        sc = super().get_search_collector()
        log.debug('Got search collector, processing data...')
        valid_gmetas = []
        for result in sc.search_data['gmeta']:
            files = result['content'][0].get('files', [])
            hdfs = len(list(filter(lambda f: f['url'].endswith('.hdf'), files)))
            imm = len(list(filter(lambda f: f['url'].endswith('.imm'), files)))
            bin = len(list(filter(lambda f: f['url'].endswith('.bin'), files)))
            all_hdf = hdfs >= 2
            rigaku = hdfs >= 1 and bin >= 1
            xpcs_lambda = hdfs >= 1 and imm >= 1
            # log.debug(f'Checks: All HDF? {all_hdf} Rigaku? {rigaku}, lambda? {xpcs_lambda}')
            if all_hdf or rigaku or xpcs_lambda:
                valid_gmetas.append(result)
        log.debug(f'Loading form with {len(valid_gmetas)}/{len(sc.search_data["gmeta"])} valid results')
        sc.search_data['gmeta'] = valid_gmetas
        return sc
