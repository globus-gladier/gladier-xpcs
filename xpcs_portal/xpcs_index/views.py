import logging
from django.urls import reverse_lazy
from django.contrib.auth.mixins import LoginRequiredMixin
from django import forms
from globus_portal_framework.views.generic import SearchView, DetailView
from globus_app_flows.views import BatchCreateView
from globus_app_flows.models import FlowAuthorization

from xpcs_portal.xpcs_index.collectors import XPCSSearchCollector, XPCSTransferCollector
from xpcs_portal.xpcs_index.forms import ReprocessDatasetsCheckoutForm
from xpcs_portal.xpcs_index.models import FilenameFilter
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


class XPCSReprocessingCheckoutView(BatchCreateView):
    """Reprocessing Checkout starts the flow immediately on verifying
    each of the subject it can process are valid"""
    form_class = ReprocessDatasetsCheckoutForm
    template_name = 'xpcs/reprocess-datasets-checkout.html'
    collector = XPCSSearchCollector
    flow = '72e6469a-cf30-46bc-bff4-94dca46f2459'
    authorization_type = "CONFIDENTIAL_CLIENT"
    # The auth key is set dynamically through the form by get_flow_authorization instead
    # authorization_key = "aps8idi-polaris"

    def get_flow_authorization(self, authorization_type: str, authorization_key: str, form: forms.Form = None) -> FlowAuthorization:
        return super().get_flow_authorization(authorization_type, form.cleaned_data['facility'], form)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['index'] = self.kwargs['index']
        return context

    def get_success_url(self):
        return reverse_lazy('search',
                            kwargs={'index': 'xpcs'})
