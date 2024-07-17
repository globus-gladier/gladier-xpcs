

def sort_cycle(facets):
    """Drop any buckets on facets with small values. This prevents
    users from gaining insights about search data with carefully crafted
    filtering."""
    for facet in facets:
        if facet.get('field_name') == 'project_metadata.cycle' and facet.get('buckets'):
            facet['buckets'].sort(key=lambda x: x.get('value'))
    return facets