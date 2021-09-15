import logging
import pathlib
import urllib
import copy
from django.urls import reverse, reverse_lazy
from django.contrib import messages
from django.shortcuts import redirect
from globus_portal_framework.views.generic import SearchView, DetailView
from concierge_app.views.generic import ManifestCheckoutView
from automate_app.models import Action, Flow
from gladier_xpcs.flow_reprocess import XPCSReprocessingFlow

from xpcs_portal.xpcs_index.forms import ReprocessDatasetsCheckoutForm
from xpcs_portal.xpcs_index.models import ReprocessingTask, FilenameFilter
from xpcs_portal.xpcs_index.apps import REPROCESSING_FLOW_DEPLOYMENT

log = logging.getLogger(__name__)


class XPCSSearchView(SearchView):
    """Custom XPCS Search view automatically filters on the xpcs-8id 'project'. This is old,
    based on the pilot project feature and will be going away eventually."""

    @property
    def filters(self):
        project_filters = [{
            'type': 'match_all',
            'field_name': 'project_metadata.project-slug',
            'values': ['xpcs-8id']
        }]
        return super().filters + project_filters


class XPCSDetailView(DetailView):
    """The custom XPCS detail view adds support for toggling images on and off"""

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        preview_list = (
            'all_preview',
            'correlation_plot_previews',
            'correlation_plot_with_fit_previews',
            'intensity_plot_previews',
            'structural_analysis_prev'
        )
        try:
            for preview in preview_list:
                for manifest in context.get(preview, []):
                    match = FilenameFilter.match(self.request.user,
                                                 manifest.get('filename'))
                    manifest['show_filename'] = match
        except Exception as e:
            log.exception(e)
        return context


class XPCSReprocessingCheckoutView(ManifestCheckoutView):
    """Reprocessing Checkout starts the flow immediately on verifying
    each of the subject it can process are valid"""
    model = ReprocessingTask
    form_class = ReprocessDatasetsCheckoutForm
    template_name = 'xpcs/reprocess-datasets-checkout.html'

    def get_search_reference_url(self):
        return self.get_success_url()

    def get_success_url(self):
        return reverse_lazy('search',
                            kwargs={'index': 'xpcs'})

    def form_valid(self, form):
        log.debug(f'Form valid for {form.__class__}')
        user = self.request.user
        sc = form.get_search_collector()
        run_inputs = [self.get_input(form.cleaned_data, REPROCESSING_FLOW_DEPLOYMENT, m)
                      for m in sc.get_manifest()]
        # HACK -- We need this to smartly choose the correct Flow, not
        # the one that was used last. This will fail for multiple deployed
        # flows.
        flow = Flow.objects.all().order_by('date_created').last()
        if not flow:
            raise Exception('No flow found, you probably need to run '
                            '"python manage.py authorize_gladier"')
        flow.batch(user, run_inputs)
        messages.success(self.request, f'Processing data in {len(run_inputs)} flow runs')

        orig_q = urllib.parse.urlparse(self.request.POST.get('search_url')).query
        new_q = f'{orig_q}&{urllib.parse.urlencode({"flow": flow.flow_id})}'
        return redirect(f'{self.get_success_url()}?{new_q}')

    def get_parameters(self, flow_input):
        return {
            'label': str(pathlib.Path(flow_input['input']['hdf_file']).parent.name)[:62],
            'manage_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
            'monitor_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
        }

    def get_input(self, cleaned_data, deployment, dataset):
        # Get filename src/dest for .hdf
        flow_input = copy.deepcopy(deployment.get_input())
        source_hdf = pathlib.Path(next(d for d in dataset if str(d).endswith('.hdf')))
        # Get filename src/dest for .imm/.bin
        source_data = pathlib.Path(next(d for d in dataset if
                                   any(str(d).endswith(s) for s in ['.imm', '.bin'])))
        source_qmap = cleaned_data['qmap_path']

        xpcs_input = XPCSReprocessingFlow().get_xpcs_input(
            deployment, source_hdf, source_data, source_qmap)
        flow_input['input'].update(xpcs_input['input'])
        flow_input['input'].update({
            'reprocessing_suffix': cleaned_data['reprocessing_suffix'],
            'qmap_source_endpoint': cleaned_data['qmap_ep'],
            'qmap_source_path': source_qmap,
        })
        return flow_input, self.get_parameters(flow_input)
