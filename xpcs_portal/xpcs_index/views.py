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

from xpcs_index.forms import (
    ReprocessingTaskForm, XPCSManifestCheckoutForm,
    ReprocessDatasetsCheckoutForm,
)
from concierge_app.views.generic import ManifestListView
from concierge_app.views.generic import ManifestCheckoutView

from xpcs_index.models import ReprocessingTask, FilenameFilter


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
        dp = self.get_deployment()
        run_inputs = [self.get_input(form.cleaned_data, m, dp)
                      for m in sc.get_manifest()]
        # HACK -- We need this to smartly choose the correct Flow, not
        # the one that was used last. This will fail for multiple deployed
        # flows.
        flow = Flow.objects.all().order_by('date_created').last()
        flow.batch(user, run_inputs)
        messages.success(self.request, f'Processing data in {len(run_inputs)} flow runs')

        orig_q = urllib.parse.urlparse(self.request.POST.get('search_url')).query
        new_q = f'{orig_q}&{urllib.parse.urlencode({"flow": flow.flow_id})}'
        return redirect(f'{self.get_success_url()}?{new_q}')

    def get_deployment(self):
        # Get isoformat of current time without microseconds
        now = datetime.datetime.now()
        now = now - datetime.timedelta(microseconds=now.microsecond)
        now = now.isoformat().replace(':', '')

        base_proc = pathlib.Path('/projects/APSDataAnalysis/nick/portal_reprocessing/')
        # base_proc = pathlib.Path('/projects/APSDataAnalysis/nick/gladier_testing/')
        theta_ep = '08925f04-569f-11e7-bef8-22000b9a448b'
        petrel_ep = 'e55b4eab-6d04-11e5-ba46-22000b92c6ec'
        return {
            'apply_qmap_funcx_id': '4a2df329-1e01-40bf-8fff-c22ff4688e17',
            'eigen_corr_funcx_id': 'ce53a0c4-cfb0-4b5c-b8aa-e1f25f6b27a7',
            'gather_xpcs_metadata_funcx_id': '6a32203d-0046-48ce-acf5-15f5d806ce72',
            'make_corr_plots_funcx_id': '26d1ca2c-9e40-43d1-af8c-c2dabdea8455',
            'publish_gather_metadata_funcx_id': '758967c7-c2db-40ae-9a8e-febac3683186',
            'publish_preparation_funcx_id': '619ee0d0-7190-4262-bc90-52cbcc6735f3',
            'warm_nodes_funcx_id': '55c2518c-d746-43fd-aa84-033a5bc38aef',

            'proc_dir': str(base_proc / now),
            # 'proc_dir': str(base_proc),
            'delete_qmap': True,
            'funcx_endpoint_non_compute': '553e7b64-0480-473c-beef-be762ba979a9',
            'funcx_endpoint_compute': '2272d362-c13b-46c6-aa2d-bfb22255f1ba',
            'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',

            'globus_endpoint_source': petrel_ep,
            'globus_endpoint_proc': theta_ep,

            'pilot': {
                # Set by flow
                # 'dataset': dataset_dir,
                'index': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
                'project': 'xpcs-8id',
                'source_globus_endpoint': theta_ep,
                # Extra groups can be specified here. The XPCS Admins group will always
                # be provided automatically.
                'groups': [],
            },
        }

    def get_parameters(self, flow_input):
        return {
            'label': str(pathlib.Path(flow_input['hdf_file']).parent.name),
            'manage_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
            'monitor_by': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
        }

    def get_input(self, cleaned_data, dataset, dp):
        # Get processing dir
        proc = pathlib.Path(dp['proc_dir'])

        # Get filename src/dest for .hdf
        source_hdf = pathlib.Path(next(d for d in dataset if str(d).endswith('.hdf')))
        # Get filename src/dest for .imm/.bin
        source_data = pathlib.Path(next(d for d in dataset if
                                   any(str(d).endswith(s) for s in ['.imm', '.bin'])))

        # Get qmap destination path
        qmap = proc / source_data.parent.name / pathlib.Path(cleaned_data['qmap_path']).name

        flow_input = {
            'qmap_source_endpoint': cleaned_data['qmap_ep'],
            'qmap_source_path': cleaned_data['qmap_path'],
            'qmap_file': str(qmap),
            'flags': '',
            'reprocessing_suffix': cleaned_data['reprocessing_suffix'],

            'hdf_file_source': str(source_hdf),
            'imm_file_source': str(source_data),
            'hdf_file': str(proc / source_hdf.parent.name / source_hdf.name),
            'imm_file': str(proc / source_data.parent.name / source_data.name),
        }
        flow_input.update(dp)
        return {'input': flow_input}, self.get_parameters(flow_input)


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
