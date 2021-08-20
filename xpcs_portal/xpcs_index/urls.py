from django.urls import path, include
from globus_portal_framework.urls import register_custom_index
from xpcs_index.views import (
    ReprocessingTaskCreate, AutomateDashboard, XPCSActionDetail,
    XPCSProjectDetail, XPCSManifestCheckoutView,
    XPCSReprocessDatasetsCheckoutView
)
from xpcs_index.api import toggle_filename_filter

app_name = 'xpcs-index'
register_custom_index('xpcs_index', ['xpcs'])

apipatterns = [
    path('filename_filter/toggle/', toggle_filename_filter, name='toggle-filename-filter'),
]

urlpatterns = [
    path('<xpcs_index:index>/xpcs-reprocess-datasets-checkout/<project>/',
         XPCSReprocessDatasetsCheckoutView.as_view(), name='xpcs-reprocess-datasets-checkout'),
    path('<xpcs_index:index>/xpcs-manifest-checkout/<project>/',
         XPCSManifestCheckoutView.as_view(), name='xpcs-manifest-project-checkout'),
    path('<xpcs_index:index>/projects/<project>/<path:subject>/',
         XPCSProjectDetail.as_view(), name='tp-project-detail'),
    path('<xpcs_index:index>/automation/',
         ReprocessingTaskCreate.as_view(), name='reprocessing-task-create'),
    path('<xpcs_index:index>/automate/',
         AutomateDashboard.as_view(), name='automate-dashboard'),
    path('<xpcs_index:index>/automate/actions/<pk>/',
         XPCSActionDetail.as_view(), name='automate-action-detail'),
    path('<xpcs_index:index>/api/', include(apipatterns)),

    # path('<xpcs_index:index>/bags/', XPCSBagListView.as_view(),
    #      name='bag-list'),
    # path('<concierge_index:index>/automation/<pk>/',
    #      Automation.as_view(), name='automation-detail'),
]
