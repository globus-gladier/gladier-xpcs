from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from xpcs_portal.xpcs_index.models import FilenameFilter


@login_required
def toggle_filename_filter(request, index):
    if request.method != 'POST':
        return JsonResponse({'error': 'method not allowed'}, 405)
    if request.POST.get('regex'):
        FilenameFilter.toggle(request.user, request.POST['regex'])
        return JsonResponse({'created': True}, status=201)
    return JsonResponse({'error': 'regex not provided'}, status=400)
