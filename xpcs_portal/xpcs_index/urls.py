from django.urls import path, include
from globus_portal_framework.urls import register_custom_index
from xpcs_portal.xpcs_index.views import (
    XPCSSearchView,
    XPCSDetailView,
    XPCSReprocessingSearchReprocessing,
    XPCSComputeTransfer,
)
from xpcs_portal.xpcs_index.api import toggle_filename_filter, operation_ls

app_name = 'xpcs-index'
register_custom_index('xpcs_index', ['xpcs'])

apipatterns = [
    path('filename_filter/toggle/', toggle_filename_filter, name='toggle-filename-filter'),
    path('operation_ls/', operation_ls, name='operation-ls'),
]

urlpatterns = [
    path('<xpcs_index:index>/', XPCSSearchView.as_view(), name='search'),
    path('<xpcs_index:index>/detail/<path:subject>/', XPCSDetailView.as_view(), name='detail'),
    path('<xpcs_index:index>/reprocessing/', XPCSReprocessingSearchReprocessing.as_view(), name='reprocessing'),
    path('<xpcs_index:index>/compute-transfer/', XPCSComputeTransfer.as_view(), name='compute-transfer'),

    path('<xpcs_index:index>/api/', include(apipatterns)),
]
