import requests

def get_token_and_catalog(auth_url, username, password, project_name, user_domain="default", project_domain="default", identity_api_version="v3", region="RegionOne"):
    """
    Authenticate with keystone, response with token and catalog data
    """
    url = f"{auth_url}/{identity_api_version}/auth/tokens"
    headers = {"Content-Type": "application/json"}
    data = {
        "auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": username,
                        "domain": {"name": user_domain},
                        "password": password
                    }
                }
            },
            "scope": {
                "project": {
                    "name": project_name,
                    "domain": {"name": project_domain}
                }
            }
        }
    }

    resp = requests.post(url, json=data, headers=headers)
    if resp.status_code != 201:
        raise Exception(f"Auth failed: {resp.text}")

    token = resp.headers["X-Subject-Token"]
    body = resp.json()
    catalog = body.get("token", {}).get("catalog", [])
    return token, catalog