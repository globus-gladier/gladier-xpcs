import logging
import pathlib
import urllib
import typing as t
import copy
from django.urls import reverse, reverse_lazy
from django.contrib import messages
from django.shortcuts import redirect
from django.contrib.auth.mixins import LoginRequiredMixin
from globus_portal_framework.views.generic import SearchView, DetailView
from globus_portal_framework.gsearch import get_pagination, get_index
from globus_portal_framework.gclients import get_user_groups
from concierge_app.views.generic import ManifestCheckoutView
from automate_app.models import Action, Flow
from gladier_xpcs.flows import XPCSReprocessingFlow

from xpcs_portal.xpcs_index.forms import ReprocessDatasetsCheckoutForm
from xpcs_portal.xpcs_index.models import ReprocessingTask, FilenameFilter
from xpcs_portal.xpcs_index.apps import REPROCESSING_FLOW_DEPLOYMENT
from xpcs_portal.xpcs_index.flows import batch_flow
from xpcs_portal.xpcs_index.mixins import PaginatedSearchView

log = logging.getLogger(__name__)


class XPCSSearchView(LoginRequiredMixin, PaginatedSearchView, SearchView):
    """Custom XPCS Search view automatically filters on the xpcs-8id 'project'. This is old,
    based on the pilot project feature and will be going away eventually."""

    @property
    def filters(self):
        return super().filters + self.get_index_info().get('default_filters', [])


class XPCSDetailView(LoginRequiredMixin, DetailView):
    """The custom XPCS detail view adds support for toggling images on and off"""

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        preview_list = (
            'all_preview',
            'correlation_plot_previews',
            'correlation_plot_with_fit_previews',
            'intensity_plot_previews',
            'structural_analysis_prev',
            'text_outputs',
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

        reprocessing = get_index(self.kwargs['index'])['reprocessing']
        flow = reprocessing['flow_id']
        try:
            run_inputs = sc.get_input_files(REPROCESSING_FLOW_DEPLOYMENT, form.cleaned_data)
            log.debug(f'Attempting to start {len(run_inputs)} flow runs.')
            batch_flow(reprocessing, run_inputs)
            messages.success(self.request, f'Processing data in {len(run_inputs)} flow runs')
        except Exception as e:
            raise
            log.exception(e)
            messages.error(self.request, f'Failed to start runs: {str(e)}')

        orig_q = urllib.parse.urlparse(self.request.POST.get('search_url')).query
        new_q = f'{orig_q}&{urllib.parse.urlencode({"flow": flow})}'
        return redirect(f'{self.get_success_url()}?{new_q}')

    def get_parameters(self, flow_input):
        return {
            'label': str(pathlib.Path(flow_input['input']['hdf_file']).parent.name)[:62],
            'manage_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
            'monitor_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
        }
