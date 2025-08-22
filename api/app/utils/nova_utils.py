import requests

def list_servers(token: str, compute_endpoint: str):
    url = f"{compute_endpoint}/servers/detail"
    headers = {
        "X-Auth-Token": token
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("servers", [])
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}

def list_host_aggregates(token: str, compute_endpoint: str):
    url = f"{compute_endpoint}/os-aggregates"
    headers = {
        "X-Auth-Token": token
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("aggregates", [])
    except requests.exceptions.RequestException as e:
        return {"error": str(e)}