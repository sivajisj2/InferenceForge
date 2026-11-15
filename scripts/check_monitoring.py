#!/usr/bin/env python3
"""Verify the gateway has been scraped and Grafana can query that data source."""
import base64
import json
import os
import time
import urllib.request

prom = f"http://127.0.0.1:{os.getenv('PROMETHEUS_PORT','9097')}"
grafana = f"http://127.0.0.1:{os.getenv('GRAFANA_PORT','3007')}"
credential = "admin:"+os.getenv("GRAFANA_ADMIN_PASSWORD","local-demo-admin")
headers = {"Authorization":"Basic "+base64.b64encode(credential.encode()).decode()}

def get(url, headers=None):
    with urllib.request.urlopen(urllib.request.Request(url,headers=headers or {}),timeout=3) as response:
        return json.load(response)

for _ in range(40):
    try:
        samples = get(prom+"/api/v1/query?query=ai_inflight_requests")["data"]["result"]
        dashboard = get(grafana+"/api/dashboards/uid/inferenceforge",headers)
        proxy = get(grafana+"/api/datasources/proxy/uid/forge-prometheus/api/v1/query?query=ai_inflight_requests",headers)
        if samples and dashboard["dashboard"]["panels"] and proxy["data"]["result"]:
            print("PASS: gateway scrape, provisioned dashboard and Grafana data-source query")
            break
    except (OSError, ValueError, KeyError):
        pass
    time.sleep(2)
else:
    raise SystemExit("Timed out waiting for monitoring data")
