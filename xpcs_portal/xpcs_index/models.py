import logging
import re
import os
import datetime
from django.db import models
from django.urls import reverse
from django.contrib.auth.models import User
from xpcs_portal.xpcs_index import filter_regexes
try:
    from gladier_xpcs.client_reprocess import XPCSReprocessingClient
except ImportError:
    XPCSReprocessingClient = None

log = logging.getLogger(__name__)


class ReprocessingTask(models.Model):

    manifest = models.ForeignKey('concierge_app.Manifest', null=True, on_delete=models.CASCADE)
    action = models.ForeignKey('automate_app.Action', on_delete=models.CASCADE)

    def get_absolute_url(self):
        return reverse('xpcs-index:automate-action-detail',
                       kwargs={'index': 'xpcs', 'pk': self.action.id})

    def generate_payload(self, user_flow_input):
        base_input = self.gladier_instance().get_input()
        now = datetime.datetime.now().isoformat().replace(':', '')
        user_input = {
            'manifest_id': str(user_flow_input['manifest'].manifest_id),
            # Old manifest id
            # 'manifest_id': '8b5c50ff-838d-4072-ad6c-ce9d142d6b04',
            'manifest_destination': 'globus://08925f04-569f-11e7-bef8-22000b9a448b/'
                                    'projects/APSDataAnalysis/nick/portal_reprocessing/'
                                    f'{now}',
            'compute_endpoint_non_compute': 'f9b73c9e-aab4-4ee2-90b4-1ac77ecf3435',
            'compute_endpoint_compute': '1a786878-a2c3-4398-9cb1-5583f437da60',
            'qmap_source_endpoint': user_flow_input['qmap_ep'],
            'qmap_source_path': user_flow_input['qmap_path'],
            'qmap_destination_endpoint': '08925f04-569f-11e7-bef8-22000b9a448b',
            'qmap_file': os.path.join(
                f'/projects/APSDataAnalysis/nick/portal_reprocessing/{now}/',
                os.path.basename(user_flow_input['qmap_path'])
            ),
            'corr_loc': '/lus/theta-fs0/projects/APSDataAnalysis/XPCS/xpcs-eigen/build/corr',
            'flags': '',
            'flat_file': 'Flatfiel_AsKa_Th5p5keV.hdf',
            'reprocessing_suffix': user_flow_input['reprocessing_suffix'],
        }
        log.debug(f'Started with manifest {user_flow_input["manifest"].manifest_id}')
        base_input['input'].update(user_input)
        self.action.payload = base_input
        self.action.save()

    @classmethod
    def gladier_instance(cls):
        if XPCSReprocessingClient is None:
            raise ValueError('Please install the xpcs_client to start XPCS flows')
        return XPCSReprocessingClient(auto_login=False, auto_registration=False)

    @classmethod
    def new_action(cls, bag, user):
        if not bag.search_collection:
            raise ValueError(f'Bag {bag} has no search collection!')
        # This snippet fetches a flow id based on the latest one used in Glaider
        # This is not an ideal choice
        # flow_id = cls.gladier_instance().get_cfg()['flow_id']
        # log.debug(f'Using flow id {flow_id} for {cls.__name__}')

        # try:
        #     flow = Flow.objects.get(flow_id=flow_id)
        # except Flow.DoesNotExist:
        #     raise ValueError(f'Flow {flow_id} has not been registered, talk '
        #                      f'to your admin to fix this.')
        # This is another hack to simply fetch the last-deployed flow. This is
        # also not ideal.
        try:
            flow = Flow.objects.all().order_by('date_created').last()
        except Exception as e:
            raise
        log.debug(f'Using flow {flow} for {user}s new XPCS Task.')
        action = Action(flow=flow, user=user)
        action.save()
        return action


class FilenameFilter(models.Model):
    """This model tracks user preferences for which images should be displayed
    for a particular search results. For XPCS, each search result is the same,
    with the same kinds of images being generated. Each type of image is
    filtered by a regex, so the same type of image (with a slightly different
    name) can be filtered out for other search results.
    Example:
      A079_AMJ290_P2VP40_S03_Ann190C_att4_175C_Lq0_001_0001-0300_intensity.png
    Where A079_AMJ290_P2VP40_S03_Ann190C_att4_175C_Lq0_001_0001-0300 is the
    name of the search result, and 'intensity.png' is the type of image, common
    across search results.
    """
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    regex = models.CharField(max_length=512)

    @classmethod
    def toggle(cls, user, filename):
        """Toggle a given filename on or off. """
        regex = filter_regexes.regex_for_filename(filename)
        obj = cls.objects.filter(user=user, regex=regex)
        if obj:
            obj.delete()
            log.debug(f'Regex deleted for {user}: {regex}')
        else:
            cls.objects.create(user=user, regex=regex)
            log.debug(f'Regex created for {user}: {regex}')

    @classmethod
    def match(cls, user, filename):
        """Return True or False if the given filename *should* be shown.
        Most images will record a 'filter' regex if the given filename should
        be shown. Some filenames override the default, and are shown by default
        but allow the user to turn them off, so a 'reverse' filter is used in
        that case.
        Returns: True if filename should be shown, False otherwise."""
        show = False
        for rfilter in cls.objects.filter(user=user):
            if re.match(rfilter.regex, filename):
                show = True
        if any(re.match(r, filename) for r in filter_regexes.SHOW_BY_DEFAULT):
            show = not show
        return show
