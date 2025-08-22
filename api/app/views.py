from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import JsonResponse
import json
from .utils.keystone_utils import get_token_and_catalog
from .utils.openstack_utils import parse_endpoints_from_catalog
from .utils.nova_utils import list_servers, list_host_aggregates

@csrf_exempt
def endpoints_view(request):
    if request.method != "POST":
        return JsonResponse({"error": "Method not allowed"}, status=405)

    data = json.loads(request.body)
    auth_url = data.get("auth_url")
    username = data.get("username")
    password = data.get("password")
    project_name = data.get("project_name")
    user_domain = data.get("user_domain", "default")
    project_domain = data.get("project_domain", "default")
    identity_api_version = data.get("identity_api_version", "v3")
    region = data.get("region", "RegionOne")

    try:
        token, catalog = get_token_and_catalog(
            auth_url, username, password, project_name, user_domain, project_domain, identity_api_version, region
        )
        endpoints = parse_endpoints_from_catalog(catalog)
        return JsonResponse({"token": token, "endpoints": endpoints})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=400)

@csrf_exempt
def servers_view(request):
    data = json.loads(request.body)
    token = data.get("token")
    compute_endpoint = data.get("compute_endpoint")

    if not token or not compute_endpoint:
        return JsonResponse({"error": "Unauthenticated, missing or expired token for API query"}, status=401)

    servers = list_servers(token, compute_endpoint)
    return JsonResponse(servers, safe=False)

@csrf_exempt
def host_aggregates_view(request):
    data = json.loads(request.body)
    token = data.get("token")
    compute_endpoint = data.get("compute_endpoint")

    if not token or not compute_endpoint:
        return JsonResponse({"error": "Unauthenticated, missing or expired token for API query"}, status=401)

    host_aggregates = list_host_aggregates(token, compute_endpoint)
    return JsonResponse(host_aggregates, safe=False)
