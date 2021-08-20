from django.contrib.sitemaps.views import sitemap
from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.shortcuts import redirect
from alcf_data_portal.views import index_selection
from alcf_data_portal.api import get_access_token

api = [
    path('access_token/', get_access_token, name='access_token')
]

urlpatterns = [
    path('api/v1/', include(api)),
    path('', include('xpcs_index.urls')),
    path('', include('alcf_data_portal.urls_tabbed_project')),
    path('', index_selection, name='index-selection'),
    path('favicon.ico', lambda r: redirect('/static/favicon.ico')),
    path('', include('social_django.urls', namespace='social')),
    path('', include('globus_portal_framework.urls')),
    path('admin/', admin.site.urls),
    # path('automate/', include('automate_app.urls', namespace='automate-app'))
]

if settings.DEBUG:
    urlpatterns += [
        path('', include('globus_portal_framework.urls_debugging'))
    ]
