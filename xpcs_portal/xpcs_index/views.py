import logging
import pathlib
import urllib
import datetime
from django.urls import reverse, reverse_lazy
from django.views.generic.edit import CreateView
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.shortcuts import redirect

from automate_app.views.action import ActionDetail
from automate_app.views.dashboard import Dashboard
from automate_app.models import Action, Flow

from alcf_data_portal.views import ProjectDetail

from xpcs_portal.xpcs_index.forms import (
    ReprocessingTaskForm, XPCSManifestCheckoutForm,
    ReprocessDatasetsCheckoutForm,
)
from concierge_app.views.generic import ManifestListView
from concierge_app.views.generic import ManifestCheckoutView

from xpcs_portal.xpcs_index.models import ReprocessingTask, FilenameFilter
from gladier_xpcs.flow_reprocess import XPCSReprocessingFlow
from xpcs_portal.xpcs_index.apps import REPROCESSING_FLOW_DEPLOYMENT

log = logging.getLogger(__name__)


class XPCSProjectDetail(ProjectDetail):

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


class ReprocessingTaskCreate(LoginRequiredMixin, CreateView):
    model = ReprocessingTask
    form_class = ReprocessingTaskForm
    template_name = 'xpcs/concierge-app/components/manifests-detail-reprocess-form.html'

    def get_absolute_url(self):
        return reverse_lazy('concierge-app:manifest-list', kwargs={'index': 'xpcs'})

    def get_success_url(self):
        return reverse_lazy('xpcs-index:automate-dashboard',
                            kwargs={'index': 'xpcs'})

    def form_valid(self, form):
        user = self.request.user
        manifest = form.cleaned_data['manifest']
        form.instance.user = user
        try:
            form.instance.action = self.model.new_action(manifest, user)
            resp = super().form_valid(form)
            # Start the Globus Automate Flow
            form.instance.generate_payload(form.cleaned_data)
            form.instance.action.start_flow()
            messages.success(self.request, 'Flow has been started')
            return resp
        except Exception as e:
            log.error(f'Error starting flow for user {user}', exc_info=True)
            messages.error(self.request, f'Error starting flow: {str(e)}')
            # Delete the empty action if it ran into an error
            if form.instance.action:
                form.instance.action.delete()
            return redirect(self.get_absolute_url())


class XPCSManifestCheckoutView(ManifestCheckoutView):
    """Manifest Limiter limits the number of results recorded by a user-provided
    number. The actual results gathered are arbitrary."""
    form_class = XPCSManifestCheckoutForm


class XPCSReprocessDatasetsCheckoutView(ManifestCheckoutView):
    """Reprocessing Checkout starts the flow immediately on verifying
    each of the subject it can process are valid"""
    model = ReprocessingTask
    form_class = ReprocessDatasetsCheckoutForm
    template_name = 'xpcs/concierge-app/tabbed-project/reprocess-datasets-checkout.html'

    def get_success_url(self):
        return reverse_lazy('tp-project-search',
                            kwargs={'index': 'xpcs',
                                    'project': self.kwargs['project']})

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
            'label': str(pathlib.Path(flow_input['input']['hdf_file']).parent.name),
            'manage_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
            'monitor_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
        }

    def get_input(self, cleaned_data, deployment, dataset):
        # Get filename src/dest for .hdf
        flow_input = deployment.get_input()
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


class XPCSManifestListView(ManifestListView, ReprocessingTaskCreate):
    pass


class AutomateDashboard(Dashboard):

    def get_actions(self):
        pk_ids = [sr.action.id for sr in ReprocessingTask.objects.all()]
        return Action.objects.filter(pk__in=pk_ids)


class XPCSActionDetail(ActionDetail):
    template_name = 'xpcs/automate_app/action_detail.html'

    def get_context_data(self, object):
        context = super().get_context_data()
        context['reprocessing_task'] = ReprocessingTask.objects.get(
            action__id=object.id
        )
        return context
