from django import forms
import json
import logging
from xpcs_portal.xpcs_index import models
from concierge_app.forms import SubjectSelectManifestCheckoutForm

from xpcs_portal.xpcs_index.search_collector import XPCSReprocessingSearchCollector

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
    #
    # def clean(self):
    #     self.cleaned_data['options_cache'] = json.dumps({
    #         'rigaku': self.data.get('rigaku', False)
    #     }, indent=2)
    #     return self.cleaned_data

    def get_search_collector(self):
        sc = super().get_search_collector()
        valid_gmetas = []
        for result in sc.search_data['gmeta']:
            hdfs = len(list(filter(lambda f: f['url'].endswith('.hdf'),
                              result['content'][0]['files'])))
            imm = len(list(filter(lambda f: f['url'].endswith('.imm'),
                             result['content'][0]['files'])))
            bin = len(list(filter(lambda f: f['url'].endswith('.bin'),
                             result['content'][0]['files'])))
            all_hdf = hdfs == 2
            rigaku = hdfs == 1 and bin == 1
            xpcs_lambda = hdfs == 1 and imm == 1
            if all_hdf or rigaku or xpcs_lambda:
                valid_gmetas.append(result)
        log.debug(f'Loading form with {len(valid_gmetas)}/{len(sc.search_data["gmeta"])} valid results')
        sc.search_data['gmeta'] = valid_gmetas
        return sc
