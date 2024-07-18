import os
from urllib.parse import urlsplit, urlencode, urlunsplit
from xpcs_portal.xpcs_index.templatetags.xpcs_filters import format_aps_cycle_v2

LISTING_PREVIEW = 'scattering_pattern_log.png'



def cherry_picked_detail(result):
    aps_cycle = get_fields([{'field': 'aps_cycle_v2', 'name': 'APS Cycle'}],
                           result[0]['project_metadata'])
    if aps_cycle:
        aps_cycle[0]['value'] = format_aps_cycle_v2(aps_cycle[0]['value'])
    all_groups = detail_field_groups(result)
    for group in all_groups:
        if group['name'] == 'Instrument Acquisition Measurements':
            if len(aps_cycle) > 0:
                group['fields'].append(aps_cycle[0])
    cherry_list = [
        'aps_cycle_v2',
        'measurement.instrument.acquisition.parent_folder',
        'measurement.instrument.acquisition.datafilename',
        'measurement.instrument.acquisition.data_folder',

        'xpcs.data_begin',
        'xpcs.data_begin_todo',
        'xpcs.data_end',
        'xpcs.data_end_todo',
        'xpcs.qmap_hdf5_filename',

        'measurement.instrument.acquisition.stage_x',
        'measurement.instrument.acquisition.stage_z',
        'measurement.instrument.acquisition.attenuation',

        'measurement.instrument.detector.exposure_time',
        'measurement.instrument.detector.exposure_period',
        'measurement.instrument.detector.manufacturer',

        'measurement.instrument.source_begin.beam_intensity_transmitted',
        'measurement.instrument.source_begin.current',
        'measurement.instrument.source_begin.datetime',
        'measurement.instrument.source_begin.energy',

        'measurement.instrument.source_end.current',
        'measurement.instrument.source_end.datetime',

        'measurement.sample.translation',
        'measurement.sample.translation_table',
        'measurement.sample.orientation',
        'measurement.sample.temperature_A',
        'measurement.sample.temperature_A_set',
    ]
    # Groups are ordered manually by the order they appear in this list.
    # Uncomment the below lines to list all available groups
    # from pprint import pprint
    # pprint([group['name'] for group in all_groups])
    group_order = [
        'Instrument Acquisition Measurements',
        'Instrument Detector Measurements',
        'Instrument Source Begin Measurements',
        'Measurement Samples',
        'XPCS'
    ]

    # Build the various categories.
    cherries = []
    for group in all_groups:
        dict_info = {'name': group['name']}
        fields = [f for f in group['fields'] if f['field'] in cherry_list]
        fields.sort(key=lambda fset: cherry_list.index(fset['field']))
        dict_info['fields'] = fields
        cherries.append(dict_info)
    cherries.sort(key=lambda g: group_order.index(g['name']))
    return cherries


def get_fields(fields, result):
    """Takes a list of dictionaries describing data to fetch, and searches
    for matches in the given result (can be any dict). If a result is not found
    no entry is placed in the return data."""
    populated_fields = []
    for field in fields:
        if result.get(field['field']):
            field['value'] = result.get(field['field'])
            populated_fields.append(field)

    return populated_fields


def get_xpcs_field_title(field_name, prefix):
    parts = field_name.replace(prefix, '').split('.')
    parts = [p.split('_') for p in parts]
    # Flatten the list, from attempting to split into lists twice.
    parts = [item for sublist in parts for item in sublist]
    return ' '.join([p.capitalize() for p in parts])


def project_metadata(result):
    return result[0]['project_metadata']


def detail_field_groups(result):
    groups = [
        ('Instrument Acquisition Measurements',
         'measurement.instrument.acquisition.'),

        ('Instrument Detector Measurements',
         'measurement.instrument.detector.'),

        ('Instrument Source Begin Measurements',
         'measurement.instrument.source_begin.'),

        ('Measurement Samples', 'measurement.sample.'),
        ('XPCS', 'xpcs.'),
    ]
    field_groups = []
    for name, group in groups:
        fields = [{
                'field': f,
                'type': 'float',
                'name': get_xpcs_field_title(f, group),
                'value': v
            }
            for f, v in result[0]['project_metadata'].items()
            if f.startswith(group)
        ]
        if fields:
            field_groups.append({'name': name, 'fields': fields})
    return field_groups


def listing_preview(result):
    for entry in fetch_all_previews(result):
        if entry['url'].endswith(LISTING_PREVIEW):
            return entry


def correlation_plot_previews(result):
    return [
        entry for entry in fetch_all_previews(result)
        if 'g2_corr' in entry['url'] and 'g2_corr_fit' not in entry['url']
    ]


def correlation_plot_with_fit_previews(result):
    return [
        entry for entry in fetch_all_previews(result)
        if 'g2_corr_fit' in entry['url'] or
           entry['url'].endswith('_corr_params.png')
    ]


def intensity_plot_previews(result):
    return [
        entry for entry in fetch_all_previews(result)
        if entry and
        entry['url'].endswith('intensity.png') or
        entry['url'].endswith('intensity_t.png')
    ]


def text_outputs(result):
    return [
        entry for entry in fetch_all_previews(result)
        if entry['mime_type'] == 'text/x-log'
    ]


def total_intensity_vs_time_preview(result):
    prev = [
        entry for entry in fetch_all_previews(result)
        if entry and entry['url'].endswith('total_intensity_vs_time.png')
    ]
    if prev:
        return prev[0]


def structural_analysis_prev(result):
    """Structural Analysis are any beamlines that aren't correlation plots
    (and also not the listing image). """
    other_prevs = (
        correlation_plot_previews(result) +
        correlation_plot_with_fit_previews(result) +
        [listing_preview(result)] +
        [total_intensity_vs_time_preview(result)] +
        intensity_plot_previews(result) +
        text_outputs(result)
    )
    other_urls = [p['url'] for p in other_prevs if p]
    previews = [entry for entry in fetch_all_previews(result)
                if entry['url'] not in other_urls]
    return previews


def fetch_all_previews(result):
    # Gather base previews from the remote file manifest
    base_previews = {
        entry['url']: {
            'caption': get_xpcs_field_title(entry['filename'].rstrip('png'),
                                            ''),
            'name': get_xpcs_field_title(entry['filename'], ''),
            'url': entry.get('https_url') or entry['url'],
            'filename': entry['filename'],
            'mime_type': entry['mime_type']
        } for entry in result[0].get('files', {})}
    # If the user provided 'preview' info, overwrite the manifest entry with
    # the 'preview' entry
    base_previews.update(
        {entry['url']: entry
         for entry in result[0]['project_metadata'].get('preview', {})})
    # Add a preview id. The preview id is used by a javascript library to
    # determine how the data should be fetched/displayed.
    previews = list(base_previews.values())
    for idx, preview in enumerate(previews):
        preview['id'] = idx
    return sorted(previews, key=lambda p: p['url'], reverse=False)


def get_full_description(result):
    try:
        return result[0]['dc']['descriptions'][0]['description']
    except KeyError:
        return ''


def get_truncated_description(result):
    size_limit = 100
    try:
        desc = get_full_description(result)
        if len(desc) > size_limit:
            desc = desc[:size_limit]
            desc += '...'
        return desc
    except Exception:
        pass


def get_file(result):
    if result[0].get('remote_file_manifest'):
        return result[0]['remote_file_manifest']
    elif result[0].get('files'):
        return result[0]['files'][0]
    return {}


def remote_file_manifest(result):
    return result[0].get('files')


def filename(result):
    return get_file(result)['filename']


def https_url(result):
    rfm = get_file(result)
    if rfm and rfm.get('url'):
        return rfm['url']


def globus_app_link(result):
    rfm = get_file(result)
    if rfm and rfm.get('url'):
        gurl = urlsplit(os.path.dirname(rfm.get('url')))
        query_params = {'origin_id': '74defd5b-5f61-42fc-bcc4-834c9f376a4f',
                        'origin_path': gurl.path}
        return urlunsplit(('https', 'app.globus.org', 'file-manager',
                           urlencode(query_params), ''))


def title(result):
    return result[0]['dc']['titles'][0]['title']


def field_metadata(result):
    metadata = get_file(result).get('field_metadata')
    if not metadata:
        return {}

    labels = metadata.get('labels', {})
    label_headers = [
        {'field': 'name', 'name': labels.get('name'), 'type': 'str'},
        {'field': 'reference', 'name': 'Reference', 'type': 'url'},
        # {'field': 'description', 'name': metadata['labels']['description']},
        # {'field': 'format', 'name': metadata['labels']['format']},
        {'field': 'type', 'name': labels.get('type'), 'type': 'str'},
        {'field': 'count', 'name': 'Count', 'type': 'int'},
        {'field': 'frequency', 'name': 'Frequency', 'type': 'int'},
        {'field': 'top', 'name': 'Top', 'type': 'str'},
        {'field': 'unique', 'name': 'Unique Items', 'type': 'int'},

        {'field': 'min', 'name': 'Minimum', 'type': 'float'},
        {'field': 'max', 'name': 'Maximum', 'type': 'float'},
        {'field': 'mean', 'name': 'Mean', 'type': 'float'},
        {'field': 'std', 'name': 'Standard Deviation', 'type': 'float'},
        {'field': '25', 'name': '25th Percentile', 'type': 'float'},
        {'field': '50', 'name': '50th Percentile', 'type': 'float'},
        {'field': '75', 'name': '75th Percentile', 'type': 'float'},
    ]

    if result[0]['project_metadata'].get('dataframe_type') == 'Matrix':
        # Remove 'reference' line
        label_headers.pop(1)
    metadata['label_headers'] = label_headers

    field_data = []
    for row in label_headers:
        row_data = [{'field': row['field'], 'value': row['name']}]
        for column in metadata.get('field_definitions', []):
            row_data.append({
                'field': row['field'],
                'value': column.get(row.get('field', '')),
                'type': row.get('type')
            })
        field_data.append(row_data)

    # It's possible we won't get any usable data from the fields. Metadata may
    # only be constrained to images, and we don't want to display that for
    # xpcs.
    data_exists = bool([row for row in field_data if len(row) > 1])
    if data_exists is False:
        return {}
    return {'fields': field_data}
