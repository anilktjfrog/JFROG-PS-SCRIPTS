#!/usr/bin/env python3
import os

import requests
from requests.auth import HTTPBasicAuth

# === CONFIGURATION ===
ARTIFACTORY_URL = "https://artifactory.jfrog.io/artifactory"
REPO_NAME = "maven-local"
USERNAME = "testuser"
PASSWORD = os.environ.get("ARTIFACTORY_PASSWORD")

AUTH = HTTPBasicAuth(USERNAME, PASSWORD)
deleted_count = 0


def get_storage_url(path: str) -> str:
    return (
        f"{ARTIFACTORY_URL}/api/storage/{REPO_NAME}/{path}"
        if path
        else f"{ARTIFACTORY_URL}/api/storage/{REPO_NAME}"
    )


def is_folder_empty(path: str) -> bool:
    url = get_storage_url(path)
    resp = requests.get(url, auth=AUTH)
    if resp.status_code != 200:
        print(f"[WARN] Failed to check folder: {url}")
        return False
    info = resp.json()
    return not info.get("children", [])


def delete_folder(path: str) -> bool:
    global deleted_count

    if path == "":
        print(f"[SKIP] Will not delete repository root: {REPO_NAME}")
        return False

    url = f"{ARTIFACTORY_URL}/{REPO_NAME}/{path}"
    print(f"[DELETE] {url}")
    resp = requests.delete(url, auth=AUTH)
    if resp.status_code in (200, 204):
        deleted_count += 1
        return True
    else:
        print(f"[WARN] Failed to delete {url}: {resp.status_code} {resp.text}")
        return False


def clean_folder(path: str) -> bool:
    """
    Recursively delete empty folders, and return True if this folder was deleted.
    """
    url = get_storage_url(path)
    resp = requests.get(url, auth=AUTH)
    if resp.status_code != 200:
        print(f"[WARN] Failed to access folder: {url}")
        return False

    info = resp.json()
    children = info.get("children", [])

    for child in children:
        name = child["uri"].lstrip("/")
        full_child_path = f"{path}/{name}" if path else name

        if child["folder"]:
            clean_folder(full_child_path)

    # Explicitly prevent deletion of repo root
    if path and is_folder_empty(path):
        return delete_folder(path)

    return False


# === EXECUTION ===
print(f"[INFO] Starting recursive cleanup in repository '{REPO_NAME}'...")
clean_folder("")  # Start from root of the repo
print(f"[DONE] Deleted {deleted_count} empty folders.")
