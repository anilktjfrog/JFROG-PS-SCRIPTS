#!/usr/bin/env python3
import os
import sys

import requests
from requests.auth import HTTPBasicAuth

# === CONFIGURATION ===
ARTIFACTORY_URL = "https://artifactory.jfrog.io/artifactory"
REPO_NAME = "maven-local"
USERNAME = "testuser"
PASSWORD = os.environ.get("ARTIFACTORY_PASSWORD")
DAYS = 3

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)


def query_old_artifacts(repo: str, days: int):
    print(f"[INFO] Querying artifacts older than {days} days in repo '{repo}'...")
    aql_query = f"""
items.find({{
  "repo": "{repo}",
  "type": "file",
  "created": {{ "$before": "{days}d" }}
}}).include("repo", "path", "name")
""".strip()

    response = requests.post(
        f"{ARTIFACTORY_URL}/api/search/aql",
        data=aql_query,
        headers={"Content-Type": "text/plain"},
        auth=AUTH,
    )

    if response.status_code != 200:
        print(f"[ERROR] AQL query failed: {response.status_code}")
        print(response.text)
        sys.exit(1)

    return response.json().get("results", [])


def delete_artifact(repo: str, path: str, name: str):
    url = f"{ARTIFACTORY_URL}/{repo}/{path}/{name}"
    print(f"[DELETE] {url}")
    response = requests.delete(url, auth=AUTH)
    if response.status_code not in (200, 204):
        print(f"[WARN] Failed to delete {url}: {response.status_code} {response.text}")


# === EXECUTION ===
artifacts = query_old_artifacts(REPO_NAME, DAYS)
print(f"[INFO] Found {len(artifacts)} files to delete.")

for item in artifacts:
    delete_artifact(item["repo"], item["path"], item["name"])

print("[DONE] Cleanup complete.")
