from django.core.management.base import BaseCommand
from xpcs_portal.xpcs_index.models import FilenameFilter
from django.contrib.auth.models import User


class Command(BaseCommand):
    help = 'Registers a globus compute Function for an Automate Flow'

    def add_arguments(self, parser):
        parser.add_argument('--purge', action='store_true', required=False)
        parser.add_argument('--user', required=False)

    def handle(self, *args, **options):
        user = options['user']
        if user:
            try:
                User.objects.get(username=user)
            except User.DoesNotExist:
                users = [u.username for u in User.objects.all()]
                print(f'No user "{options["user"]}" found, please choose from '
                      f'the following: {users}')
                return
            filters = FilenameFilter.objects.filter(user__username=user)
        else:
            filters = FilenameFilter.objects.all()
        filters.order_by('user')
        if options['purge']:
            print(f'Deleted {filters.delete()} filters.')
        else:
            for f in filters:
                print(f'{f.user} -- {f.regex}')
            if not filters:
                print('No filters set by any user.')
