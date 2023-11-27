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
