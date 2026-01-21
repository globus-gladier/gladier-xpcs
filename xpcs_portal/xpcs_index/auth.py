import logging
import globus_sdk
from django.conf import settings
from django.contrib.auth.models import User

log = logging.getLogger(__name__)


def get_globus_app_client(user: User, globus_app_client: str):
    if user.username != "nickolaussaint@globusid.org":
        raise Exception("Auth not implemented!")
    log.warning("Danger zone, requesting CC creds on unimplemented auth groups")
    cc = settings.GLOBUS_APP_FLOWS_AUTHORIZATIONS["confidential_client"][globus_app_client]
    app = globus_sdk.ClientApp("CC_AUTH", client_id=cc["client_id"], client_secret=cc["client_secret"])
    return app
