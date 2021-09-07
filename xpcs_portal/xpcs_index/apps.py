import os
# from django.conf import settings
from django.apps import AppConfig
from xpcs_portal.xpcs_index import fields
from gladier_xpcs.deployments import NickPortalDeployment

APP_DIR = os.path.join(os.path.dirname(__file__))


class XPCSIndexConfig(AppConfig):
    name = 'xpcs_portal.xpcs_index'


GLADIER_CFG = os.path.abspath(os.path.join(APP_DIR, 'gladier.cfg'))
RESOURCE_SERVER = 'petrel_https_server'
# RESOURCE_SERVER = 'c7683485-3c3f-454a-94c0-74310c80b32a'
REPROCESSING_FLOW_DEPLOYMENT = NickPortalDeployment()

SEARCH_INDEXES = {
    'xpcs': {
        'uuid': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
        'name': 'APS XPCS',
        # 'tagline': 'APS Beamline Data',
        'group': '',
        'base_templates': 'globus-portal-framework/v2/',
        'tabbed_project': False,
        'access': 'private',
        'template_override_dir': 'xpcs',
        'description': (
            'X-ray Photon Correlation Spectroscopy (XPCS) is a technique to '
            'study dynamics in materials at nanoscale by identifying '
            'correlations in time series of area detector images'
        ),
        'fields': [
            ('title', fields.title),
            ('truncated_description',
             fields.get_truncated_description),
            ('description', fields.get_full_description),
            ('search_results', fields.search_results),
            ('detail_field_groups', fields.detail_field_groups),
            ('field_metadata', fields.field_metadata),
            ('globus_app_link', fields.globus_app_link),
            ('filename', fields.filename),
            ('remote_file_manifest',
             fields.remote_file_manifest),
            ('cherry_picked_detail', fields.cherry_picked_detail),
            'dc',
            'ncipilot',
            ('https_url', fields.https_url),
            ('copy_to_clipboard_link', fields.https_url),
            ('resource_server', lambda r: RESOURCE_SERVER),
            ('project_metadata', fields.project_metadata),
            ('all_preview', fields.fetch_all_previews),
            ('listing_preview', fields.listing_preview),
            ('total_intensity_vs_time_preview',
             fields.total_intensity_vs_time_preview),
            ('correlation_plot_previews',
             fields.correlation_plot_previews),
            ('correlation_plot_with_fit_previews',
             fields.correlation_plot_with_fit_previews),
            ('intensity_plot_previews', fields.intensity_plot_previews),
            ('structural_analysis_prev', fields.structural_analysis_prev),
        ],
        'facets': [
            {
                "name": "Creator",
                "field_name": "dc.creators.creatorName",

            },
            {
                "name": "Parent Folder",
                "field_name": "project_metadata.parent",
            },
            {
                "name": "APS Cycle",
                "field_name": "project_metadata.cycle",
            },
            {
                "name": "Dates",
                "field_name": "dc.dates.date",
                "type": "date_histogram",
                "date_interval": "day",
            },
            {
                "name": "Qmap",
                "field_name": "project_metadata.reprocessing.qmap.name",
            },
            {
                "name": "Reprocessed Datasets",
                "field_name": "project_metadata.reprocessing.suffix",
            },
        ],
        # 'result_format_version': '2019-08-27',
    }
}
