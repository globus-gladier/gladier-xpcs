import logging
import globus_sdk
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from xpcs_portal.xpcs_index.models import FilenameFilter
from globus_portal_framework.gclients import load_transfer_client
from xpcs_portal.xpcs_index.auth import get_globus_app_client

log = logging.getLogger(__name__)


@login_required
def toggle_filename_filter(request, index):
    if request.method != 'POST':
        return JsonResponse({'error': 'method not allowed'}, 405)
    if request.POST.get('regex'):
        FilenameFilter.toggle(request.user, request.POST['regex'])
        return JsonResponse({'created': True}, status=201)
    return JsonResponse({'error': 'regex not provided'}, status=400)


@login_required
def operation_ls(request, index):

    required = ['collection', 'path']
    for item in required:
        if item not in request.GET:
            return JsonResponse({'error': f'Missing param: {item}'}, status=400)

    try:
        globus_app_client = request.GET.get("service_account")
        if globus_app_client:
            app = get_globus_app_client(request.user, globus_app_client)
            tc = globus_sdk.TransferClient(app=app)
        else:
            tc = load_transfer_client(request.user)
        response = tc.operation_ls(request.GET["collection"], path=request.GET["path"])
        return JsonResponse(response.data)
    except Exception as e:
        log.error('Unexpected error querying path')
        log.exception(e)
        return JsonResponse({'error': str(e)}, status=500)
