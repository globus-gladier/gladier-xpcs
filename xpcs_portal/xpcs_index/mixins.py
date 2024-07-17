import math
from globus_portal_framework.views.generic import SearchView
from globus_portal_framework.gsearch import get_index



class PaginatedSearchView(SearchView):
    results_per_page = 50
    # Maximum offset defined in Globus Search
    maximum_pagination = 10000

    def __init__(self, *args, **kwargs):
        kwargs['results_per_page'] = self.results_per_page
        super().__init__(*args, **kwargs)
        
    def process_result(self, *args, **kwargs):
        # Include pagination in results
        result = super().process_result(*args, **kwargs)
        result['search']['pagination'] = self.get_pagination(result['search']['total'],
                                                             result['search']['offset'],
                                                             self.get_results_per_page())
        return result

    @property
    def page(self):
        max_page = self.maximum_pagination // self.results_per_page
        page = super().page
        if int(page) > max_page:
            page = max_page
        return str(page)


    def get_results_per_page(self):
        try:
            index_data = self.get_index_info()
            return index_data.get('results_per_page', self.results_per_page)
        except AttributeError:
            return self.results_per_page

    def get_pagination(self, total_results, offset, per_page):
        page_count = math.ceil(total_results / per_page) or 1
        max_page = self.maximum_pagination // per_page
        all_pages = [
            {
                'number': p + 1,
            } for p in range(page_count) if p < max_page
        ]
        current_page = offset // per_page + 1
        if len(all_pages) <= 10:
            # If we can fit all pages on one screen, do that.
            pages = all_pages
        else:
            # We have more pages than can fit on the screen.
            # pagination should show a few pages ahead and behind the users
            # current location. These are bracketed with 'low' and 'high'.
            num_pages = 10
            low_bracket = current_page - num_pages // 2 - 1
            high_bracket = current_page + num_pages // 2
            # Shift pages to the higher bracket if there aren't enough 'previous' pages
            # This allows us to continue showing the same amount of pages on screen.
            if low_bracket < 0:
                high_bracket += abs(low_bracket)
                low_bracket = 0

            pages = all_pages[low_bracket: high_bracket]

            # Ensure the first and last pages always exist, so the user
            # can jump to either end quickly
            if low_bracket > 0:
                pages[0] = all_pages[0]
            if high_bracket < max_page:
                pages[-1] = all_pages[-1]
            pages = pages
        return {
            'current_page': offset // per_page + 1,
            'pages': pages,
            'current_range': {
                'low': offset,
                'high': offset + per_page if offset + per_page < total_results else total_results
                }
        }
