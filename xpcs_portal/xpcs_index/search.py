import logging
import copy
import urllib
from globus_portal_framework import gsearch
from globus_portal_framework.gclients import load_search_client

log = logging.getLogger(__name__)


class SearchCollector(object):
    """A Search Collector is an object intended to understand and process data
    for a given index. Since each Globus Search Index can store data a little
    bit differently, this can be overridden to account for differences one
    index has over another."""
    DEFAULT_SEARCH_KWARGS = {'limit': 10000,
                             'filters': []}
    # PROJECT_FILTER = {
    #     'field_name': 'project_metadata.project-slug',
    #     'type': 'match_all',
    #     'values': []
    # }
    # Supported algorithms in the remote file manifest.
    EXPECTED_ALGORITHMS = ['sha256']
    # For each search record, where is the file manifest? Used by
    # get_manifest() for indices that natively support Remote File Manifests
    MANIFEST_KEY = 'files'

    def __init__(self, index, name='', query=None, filters=None, project=None,
                 search_data=None, user=None, search_kwargs=None,
                 metadata=None):
        self.index = index
        self.name = name
        self.query = query
        self.filters = filters or []
        self._search_data = search_data or {}
        self._has_searched = False
        self.project = project or ''
        self.user = user
        self.search_kwargs = search_kwargs or {}
        self.metadata = metadata or {}

    @property
    def search_data(self):
        if self._search_data and self._search_data.get('gmeta'):
            return self._search_data
        required = [self.index, self.query]
        if not all(required):
            log.warning('Not enough data to conduct search, populating '
                        f'collection with empty values (user: {self.user}')
            self._search_data = {'gmeta': [], 'count': 0}
            return self._search_data
        if self._has_searched is False:
            self._search_data = self.post_search()
            # Ensure only one search is done
            self._has_searched = True
        log.debug(f'{self.__class__}: Fetching {len(self._search_data["gmeta"])} Results')
        return self._search_data

    @search_data.setter
    def search_data(self, value):
        self._search_data = value

    def post_search(self):
        """Gather results from Globus Search. Wraps
        globus_portal_framework.gsearch.post_search with optional ability to
        specify a project. Default filters can easily be added.
        **parameters**
        ``user`` The user, typically from request.user. None for unauthorized
          search.
        ``index`` The index name defined in settings.SEARCH_INDEXES
        ``project`` Filter based on project, if this index supports it. None
          for no project filter.
        ``kwargs`` Extra post_search kwargs. By Default, q=* and limit=10000
          are used.
        **returns**
        The result from globus_portal_framework.gsearch.post_search()
        **Example**
        post_search(
          'my-foo-index',
          request.user,
          'my-project-name'
          search_kwargs={q='bananas'}
          q='*',
          filters=globus_portal_framework.gsearch.get_search_filters(request),
          limit=1000
        )
        """
        skwargs = copy.deepcopy(self.DEFAULT_SEARCH_KWARGS)
        if skwargs['filters']:
            skwargs['filters'] += self.filters
        else:
            skwargs['filters'] = self.filters
        skwargs.update(self.search_kwargs or {})
        if self.project:
            log.debug(f'Appending project filter for {self.project}')
            pf = copy.deepcopy(self.PROJECT_FILTER)
            pf['values'].append(self.project)
            skwargs['filters'].append(pf)

        skwargs['q'] = skwargs.get('q', self.query or '*')

        sc = load_search_client(self.user)
        index_data = gsearch.get_index(self.index)
        sdata = sc.post_search(index_data['uuid'], skwargs).data
        log.debug(f'Did post search with {self.__class__}: {skwargs}')
        return sdata

    def process_search_data(self):
        fields = gsearch.get_index(self.index).get('fields', [])
        psd = gsearch.process_search_data(fields, self.search_data['gmeta'])
        return {
            'search_results': psd,
            'count': self.search_data.get('count', 0),
            'total': self.search_data.get('total', 0),
        }

    def get_record(self, gmeta):
        return gmeta['entries'][0]['content']

    def get_dc_block(self):
        creators_set = set()
        creators = list()
        formats = set()
        subjects = set()
        for search_entry in self.search_data['gmeta']:
            content = self.get_record(search_entry)

            # Add Creators
            for cr in content['dc']['creators']:
                if cr['creatorName'] not in creators_set:
                    creators.append(cr)
                    creators_set.add(cr['creatorName'])

            # Add Formats
            formats.update(set(content['dc']['formats']))

            # Add Subjects
            subjects.update({s['subject'] for s in content['dc']['subjects']})
        return {
            'creators': creators,
            'subjects': [{'subject': s} for s in subjects],
        }

    def get_sources(self):
        # Make these Globus URLs instead of HTTP urls
        return [m['url'] for m in self.get_manifest()]

    @staticmethod
    def remote_file_manifest_to_globus_manifest(remote_file_manifest):
        manifest = []
        algs = ['sha512', 'sha256', 'sha1', 'md5']
        for rfm in remote_file_manifest:
            hashes = [{'algorithm': k, 'value': v} for k, v in rfm.items()
                      if k in algs]
            hashes.sort(key=lambda x: algs.index(x['algorithm']))
            ent = {'source_ref': rfm['url'], 'dest_path': rfm['filename']}
            if hashes:
                ent['checksum'] = hashes[0]
            manifest.append(ent)
        return manifest

    def get_manifest(self):
        new_manifests = list()
        for raw_record in self.search_data['gmeta']:
            try:
                new_manifest = list()
                record = self.get_record(raw_record)
                if not record.get(self.MANIFEST_KEY):
                    log.warning(f'{self.name}: No manifest found for '
                                f'search data.')
                man_keys = ['url', 'length', 'filename', 'md5', 'sha256']
                for man in record[self.MANIFEST_KEY]:
                    new_man = copy.deepcopy(man)
                    new_man = {k: v for k, v in new_man.items() if k in man_keys}
                    new_manifest.append(new_man)
                new_manifests.append(new_manifest)
            except Exception as e:
                log.debug(f'Failed to fetch data for record '
                          f'Sub: {raw_record["subject"]} Key: {str(e)}')
        return new_manifests

    def get_subjects(self):
        return [e['subject'] for e in self.search_data['gmeta']]

    def get_manifest_metadata(self):
        meta = {
            'created_by': 'https://petreldata.net',
            'search_index': self.index,
            'search_url': 'Currently Unavailable',
            'name': self.name,
            'files': len(self.get_manifest()),
        }
        meta.update(self.metadata)
        return meta

    def prune_partial_hashes(self, manifest):
        """
        Given a manifest: [{'md5': 'abcd', 'sha256': 'efgh'}, ...], remove any
        hashes if the hash is not present in each of the manifest entries.
        This is required for some operations, like staging bags via the
        concierge service, where full hash coverage is needed or the operation
        will fail.
        :param manifest:
        :return:
        """
        for algorithm in self.EXPECTED_ALGORITHMS:
            alg_in_manifest = [bool(man.get(algorithm)) for man in manifest]
            if not all(alg_in_manifest):
                log.warning(
                    '{} only exists for some records! Pruning it from the '
                    'list of manifests.'.format(algorithm))
                for man in manifest:
                    if man.get(algorithm):
                        man.pop(algorithm)
        return manifest
