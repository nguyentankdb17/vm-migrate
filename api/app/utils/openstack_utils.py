def parse_endpoints_from_catalog(catalog):
    endpoints = []
    for service in catalog:
        service_name = service.get("name")
        service_type = service.get("type")
        for ep in service.get("endpoints", []):
            endpoints.append({
                "service_name": service_name,
                "service_type": service_type,
                "interface": ep.get("interface"),
                "region": ep.get("region"),
                "url": ep.get("url"),
            })
    return endpoints