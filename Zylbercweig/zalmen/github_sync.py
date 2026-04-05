"""
GitHub API helper for persisting TSV files across Streamlit Cloud redeploys.

Usage:
    from zalmen.github_sync import push_file_to_github
    push_file_to_github("Zylbercweig/organizations/org_alignment_review.tsv", local_path)
"""

from __future__ import annotations

import base64
import pathlib

import requests
import streamlit as st


def push_file_to_github(repo_path: str, local_path: pathlib.Path, commit_message: str) -> bool:
    """
    Commit a local file to GitHub via the Contents API.

    Args:
        repo_path:      Path within the repo (e.g. "Zylbercweig/organizations/foo.tsv")
        local_path:     Absolute path to the file on the local/cloud filesystem
        commit_message: Git commit message

    Returns True on success, False if credentials are missing or the API call fails.
    """
    try:
        token = st.secrets.get("github_token", "")
        repo  = st.secrets.get("github_repo", "")
        branch = st.secrets.get("github_branch", "main")
    except Exception:
        return False

    if not token or not repo:
        return False

    try:
        with open(local_path, "rb") as f:
            content = f.read()

        url = f"https://api.github.com/repos/{repo}/contents/{repo_path}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

        # Fetch current SHA (required by GitHub API to update an existing file)
        get_resp = requests.get(url, headers=headers, params={"ref": branch}, timeout=10)
        sha = get_resp.json().get("sha") if get_resp.ok else None

        payload: dict = {
            "message": commit_message,
            "content": base64.b64encode(content).decode(),
            "branch": branch,
        }
        if sha:
            payload["sha"] = sha

        put_resp = requests.put(url, json=payload, headers=headers, timeout=15)
        return put_resp.ok

    except Exception:
        return False
