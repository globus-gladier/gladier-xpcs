from django import forms
import json
import logging
import os
from crispy_forms.helper import FormHelper
from crispy_forms.layout import Layout, Fieldset, Submit, HTML
from xpcs_portal.xpcs_index import models
from concierge_app.forms import SubjectSelectManifestCheckoutForm

from xpcs_portal.xpcs_index.search_collector import XPCSReprocessingSearchCollector

log = logging.getLogger(__name__)


class ReprocessDatasetsCheckoutForm(SubjectSelectManifestCheckoutForm):
    SEARCH_COLLECTOR_CLASS = XPCSReprocessingSearchCollector
    CHOICES = [("alcf", "ALCF"), ("nersc", "NERSC")]
    QMAP_CHOICES = [(os.path.join("/XPCSDATA/partitionMapLibrary/2019-1/", p), p) for p in [
        "Rigaku_test.h5",
        "Rigaku_test_2.h5",
        "Rigaku_test_3.h5",
        "comm201901_qmap_aerogel_Lq0.h5",
        "conrad201902_qmap_Aerogel_Lq0_S360_D72.h5",
        "conrad201902_qmap_Star_S260_D36_lin.h5",
        "conrad201902_qmap_Star_S260_D36_lin_D061.h5",
        "conrad201902_qmap_polymer_Lq0_S270_D54_log.h5",
        "conrad201902_qmap_polymer_Lq0_S360_D36_log.h5",
        "deleteme.h5",
        "dufresne201903_qmap_Latex_UFXC.h5",
        "dufresne201904_qmap_Rigaku_Rq0_test.h5",
        "dufresne201904_qmap_Rigaku_Rq0_test_full.h5",
        "dufresne201904_qmap_Rigaku_beamstop.h5",
        "dufresne201904_qmap_Rigaku_beamstop.h5.h5",
        "dufresne201904_qmap_Rigaku_blemish.h5",
        "dufresne201904_qmap_Rigaku_blemish.h5.h5",
        "dufresne201904_qmap_Rigaku_full.h5",
        "dufresne201904_qmap_Silica_Rq1.h5",
        "dufresne201904_qmap_Silica_Uq0.h5",
        "dufresne201904_qmap_Silica_donut_Rq1.h5",
        "fenter201902_qmap_test_S180_D36.h5",
        "foster201902_qmap_A312_phi_horizontal_fine_5deg.h5",
        "foster201902_qmap_A312_phi_vertical_5deg.h5",
        "foster201902_qmap_SMB_A_BR_S180_18_D36_18.h5",
        "foster201902_qmap_SMB_A_BR_S270_D54.h5",
        "foster201902_qmaps.zip",
        "gadikota201903_qmap_laponite_Lq0.h5",
        "gadikota_qmap_laponite.h5",
        "harden201902_qmap_Aerogel_Lq0_S360_D72.h5",
        "harden201902_qmap_Ludoxmixture_Lq2_s180_d18.h5",
        "harden201902_qmap_Ludoxmixture_Lq2_vor_s120_d12.h5",
        "harden201902_qmap_SilicaBeads_Lq1_Full_S360_D72.h5",
        "harden201902_qmap_ludoxmixture_Lq2_s360_d36.h5",
        "harden201902_qmap_ludoxmixture_Lq2_s90_d9.h5",
        "harden201902_qmap_ludoxmixture_Lq2_vor_s60_d6.h5",
        "harden201902_qmap_silicabeads_Lq1_flow_s180_d18.h5",
        "hwang20190414_qmap_Silica_Rq1.h5",
        "hwang20190414_qmap_Silica_Rq1.h5.h5",
        "hwang201904_qmap_Silica_Rq1.h5",
        "hwang201904_qmap_Silica_Rq1_2.h5.h5",
        "hwang201904_qmap_Silica_Uq1.h5.h5",
        "mckinley201902_qmap_Aerogel_Lq0_S360_D72.h5",
        "mckinley201902_qmap_Gel_Lq1_S360_D72.h5",
        "mckinley201902_qmap_sampleid_Sq1.h5",
        "mckinley_D72_D8_S360_S16_Lin.h5",
        "mill_silicaSAXS.h5",
        "samsri201903_qmap_A117_Lq1_S360_18_D72_1.h5",
        "samsri201903_qmap_A117_Lq1_S360_36_D72_1.h5",
        "samsri201903_qmap_A117_Lq1_S360_54_D72_1.h5",
        "samsri201903_qmap_Advait_Lq0_S270_D54.h5",
        "samsri201903_qmap_Aerogel_Lq0_S360_D72.h5",
        "samsri201903_qmap_Latex_Uq0_New_S180_D18.h5",
        "samsri201903_qmap_Latex_Uq0_S180_D18.h5",
        "samsri201903_qmap_Latex_Uq1_New_S180_D18.h5",
        "samsri201903_qmap_PECgel_Lq1_S360_120_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S360_36_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S360_72_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S360_D72.h5",
        "samsri201903_qmap_PECgel_Lq1_S504_120_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S504_72_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S720_120_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S720_36_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_S720_72_D72_1.h5",
        "samsri201903_qmap_PECgel_Lq1_ring_S180_72_D18_1.h5",
        "samsri201903_qmap_PECgel_Lq1_ring_S180_72_D36_1.h5",
        "sanat201903_qmap_A037_qpeak_18phis.h5",
        "sanat201903_qmap_S270_D54_lin.h5",
        "sinha_qmap_Au0p1_full_Sq1.h5",
        "stephenson_qmap_LatexDilute_Log_2_Uq0.h5",
        "stephenson_qmap_LatexDilute_Log_Uq0.h5",
        "stephenson_qmap_nosample_Uq0.h5",
        "winey201903_qmap_Lq0.h5",
        "winey201903_qmap_S270_D54.h5",
        "zjiang201902_qmap_Generic_FullImage_Lambda.h5",
    ]]
    HIDDEN_FIELDS = [
        'index',
        'name',
        'project',
        'query',
        'filters',
        'search_url',
        'qmap_ep',
    ]
    

    facility = forms.ChoiceField(choices=CHOICES)
    options_cache = forms.CharField(label='Options', required=False)
    qmap_ep = forms.CharField(initial="74defd5b-5f61-42fc-bcc4-834c9f376a4f", widget=forms.TextInput(attrs={"readonly": True}))
    qmap_parameter_file = forms.ChoiceField(choices=QMAP_CHOICES)
    reprocessing_suffix = forms.CharField(required=False)

    class Meta:
        model = models.ReprocessingTask
        fields = ['query', 'options_cache', 'qmap_ep', 'qmap_path', 'reprocessing_suffix']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.helper = FormHelper()
        self.helper.layout = Layout(
            Fieldset(
                'Settings for reprocessing data at the selected facility',
                'facility',
                'qmap_parameter_file',
                *self.HIDDEN_FIELDS,
                HTML("""
                    <div class="alert alert-secondary" role="alert">
                    {% for result in form.get_search_collector.process_search_data.search_results %}
                        <div class="form-check my-1">
                        <div class="input-group">
                            <input class="form-check-input subject-checkbox" type="checkbox" name="subjects"
                                id="form-input-search-record-{{forloop.counter}}" value="{{result.subject}}"
                                aria-describedby="publishHelpBlock"
                                {% if not form.subjects.value or result.subject in form.subjects.value %} checked{% endif %}
                            >
                            <label class="form-check-label" style="min-width: 30%" for="form-input-search-record-{{forloop.counter}}">{{result.title|truncatechars:100}}</label>
                            <div>
                            <button type="button" class="btn btn-primary btn-sm ml-1 py-0" data-toggle="collapse" data-target="#collapse-search-record-{{forloop.counter}}" aria-expanded="true" aria-controls="collapse-search-record-{{forloop.counter}}">
                                <i class="fas fa-info-circle"></i><span class="pl-2">Info</span>
                            </button>
                            </div>
                        </div>
                        <div id="collapse-search-record-{{forloop.counter}}" class="collapse" aria-labelledby="heading-search-record-{{forloop.counter}}" data-parent="#accordion-main-checkout">
                            <div class="card-body">
                            {% include 'xpcs/globus-portal-framework/v2/components/search-result.html' %}
                            </div>
                        </div>
                        </div>
                        <hr class="my-0">
                        {% empty %}
                        {% block checkout_form_no_search_results %}
                        <div class="alert alert-info" role="alert">
                        <h5>No Search Results</h5>

                        <p>
                            No valid search results could be found.
                        </p>

                        </div>
                        {% endblock %}
                    {% endfor %}
                    </div>
                """)
            ),
            Submit('submit', 'Submit', css_class='button white'),
        )
        for field in self.HIDDEN_FIELDS:
            self.fields[field].widget = forms.HiddenInput()
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
                              result['entries'][0]['content'].get('files', []))))
            imm = len(list(filter(lambda f: f['url'].endswith('.imm'),
                             result['entries'][0]['content'].get('files', []))))
            bin = len(list(filter(lambda f: f['url'].endswith('.bin'),
                             result['entries'][0]['content'].get('files', []))))
            all_hdf = hdfs == 2
            rigaku = hdfs == 1 and bin == 1
            xpcs_lambda = hdfs == 1 and imm == 1
            if all_hdf or rigaku or xpcs_lambda:
                valid_gmetas.append(result)
        log.debug(f'Loading form with {len(valid_gmetas)}/{len(sc.search_data["gmeta"])} valid results')
        sc.search_data['gmeta'] = valid_gmetas
        return sc
