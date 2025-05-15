import logging
import urllib
import pathlib
from django.urls import reverse_lazy, reverse
from django.views.generic.edit import FormView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib import messages
from django import forms
from globus_portal_framework.views.generic import SearchView, DetailView
from globus_app_flows.views import BatchCreateView
from globus_app_flows.models import FlowAuthorization
from globus_app_flows.models import Batch, Collector, Flow, FlowAuthorization

from xpcs_portal.xpcs_index.collectors import (
    XPCSSearchCollector,
    XPCSTransferCollector,
    XPCSSuffixSearchCollector,
)
from xpcs_portal.xpcs_index.forms import (
    ReprocessDatasetsCheckoutForm,
    CollectionSelectionForm,
)
from xpcs_portal.xpcs_index.models import FilenameFilter
from xpcs_portal.xpcs_index.mixins import PaginatedSearchView

log = logging.getLogger(__name__)


class XPCSSearchView(LoginRequiredMixin, PaginatedSearchView, SearchView):
    """Custom XPCS Search view automatically filters on the xpcs-8id 'project'. This is old,
    based on the pilot project feature and will be going away eventually."""

    @property
    def filters(self):
        return super().filters + self.get_index_info().get("default_filters", [])


class XPCSDetailView(LoginRequiredMixin, DetailView):
    """The custom XPCS detail view adds support for toggling images on and off"""

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        preview_list = (
            "all_preview",
            "correlation_plot_previews",
            "correlation_plot_with_fit_previews",
            "intensity_plot_previews",
            "structural_analysis_prev",
            "text_outputs",
        )
        try:
            for preview in preview_list:
                for manifest in context.get(preview, []):
                    match = FilenameFilter.match(
                        self.request.user, manifest.get("filename")
                    )
                    manifest["show_filename"] = match
        except Exception as e:
            log.exception(e)
        return context


class XPCSReprocessing(object):
    """Reprocessing Checkout starts the flow immediately on verifying
    each of the subject it can process are valid"""

    form_class = ReprocessDatasetsCheckoutForm
    template_name = "xpcs/reprocess-datasets-checkout.html"
    flow = "72e6469a-cf30-46bc-bff4-94dca46f2459"
    authorization_type = "CUSTOM"
    group = "368beb47-c9c5-11e9-b455-0efb3ba9a670"
    # The auth key is set dynamically through the form by get_flow_authorization instead
    # authorization_key = "aps8idi-polaris"

    def get_flow_authorization(
        self, authorization_type: str, authorization_key: str, form: forms.Form = None
    ) -> FlowAuthorization:
        akey = {
            "aps8idi-polaris": "CONFIDENTIAL_CLIENT",
            "aps8idi-polaris-backup": "USER",
        }
        return super().get_flow_authorization(
            akey[form.cleaned_data["facility"]], form.cleaned_data["facility"], form
        )

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context["index"] = self.kwargs["index"]
        return context

    def get_success_url(self):
        url = reverse_lazy("search", kwargs={"index": "xpcs"})
        return f"{url}?flow={self.flow}"


class XPCSReprocessingSearchReprocessing(XPCSReprocessing, BatchCreateView):
    # collector = XPCSSearchCollector
    collector = XPCSSuffixSearchCollector


# class XPCSComputeTransfer(XPCSReprocessing, BatchCreateView):
#     collector = XPCSTransferCollector


class XPCSComputeTransfer(XPCSReprocessing, BatchCreateView):
    template_name = "xpcs/transfer-selection-form.html"
    form_class = CollectionSelectionForm
    collector = XPCSTransferCollector
    collection = "74defd5b-5f61-42fc-bcc4-834c9f376a4f"
    path = "/XPCSDATA/"
    cycle_exclude = [
        "spec_data",
        "partitionMapLibrary",
        "MDF",
        "AutomateTesting",
        "Automate",
    ]
    dataset_exclude = ["ALCF_results", "cluster_results", "logs"]

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context["index"] = self.kwargs["index"]
        context.update(
            {
                "collection": self.collection,
                "path": self.path,
                "cycle_exclude": self.cycle_exclude,
                "dataset_exclude": self.dataset_exclude,
            }
        )
        return context

    def get_collector(self, collection: str, path: str) -> XPCSTransferCollector:
        ctype = self.get_collector_class().get_import_string()
        collector = Collector(
            data=dict(collection=collection, path=path),
            user=self.request.user,
            collector_type=ctype,
        )
        return collector

    def form_valid(self, form):
        response = FormView.form_valid(self, form)
        authorization = self.get_flow_authorization(
            self.authorization_type, self.authorization_key, form=form
        )
        path = (
            pathlib.Path(self.path)
            / form.cleaned_data["cycle"]
            / form.cleaned_data["parent"]
        )
        collector = self.get_collector(self.collection, str(path))
        collector.save()
        batch = self.get_batch(authorization, collector, form)
        batch.save()

        messages.success(self.request, "Started processing for new flow runs.")
        return response
