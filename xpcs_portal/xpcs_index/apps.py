import os
from django.conf import settings
from django.apps import AppConfig
from xpcs_portal.xpcs_index import fields
from gladier_xpcs.deployments import deployment_map

APP_DIR = os.path.join(os.path.dirname(__file__))


class XPCSIndexConfig(AppConfig):
    name = 'xpcs_portal.xpcs_index'


GLADIER_CFG = os.path.abspath(os.path.join(APP_DIR, 'gladier.cfg'))
RESOURCE_SERVER = '74defd5b-5f61-42fc-bcc4-834c9f376a4f'
# RESOURCE_SERVER = 'c7683485-3c3f-454a-94c0-74310c80b32a'

AVAILABLE_DEPLOYMENTS = {
    'aps8idi-polaris',
    # 'nersc'
    'aps8idi-polaris-backup',
}

SEARCH_INDEXES = {
    'xpcs': {
        'uuid': '6871e83e-866b-41bc-8430-e3cf83b43bdc',
        'name': 'APS XPCS',
        # 'tagline': 'APS Beamline Data',
        'results_per_page': 50,
        'group': '',
        'base_templates': 'globus-portal-framework/v2/',
        'tabbed_project': False,
        'reprocessing_flow': {
            'flow_id': '72e6469a-cf30-46bc-bff4-94dca46f2459',
            'flow_scope': 'https://auth.globus.org/scopes/72e6469a-cf30-46bc-bff4-94dca46f2459/flow_72e6469a_cf30_46bc_bff4_94dca46f2459_user',
            'group': '368beb47-c9c5-11e9-b455-0efb3ba9a670',
        },
        'reprocessing_enabled': True,
        'access': 'private',
        'template_override_dir': 'xpcs',
        # Automatically append these filters to all searches
        # Currently, this is used to hide a globus-pilot record used for tracking project data
        'default_filters': [{
            'type': 'match_all',
            'field_name': 'project_metadata.project-slug',
            'values': ['xpcs-8id']
        }],
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
            ('text_outputs', fields.text_outputs),
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
        'facet_modifiers': [
            'xpcs_portal.xpcs_index.modifiers.sort_cycle',
        ],
        # 'result_format_version': '2019-08-27',
    }
}
