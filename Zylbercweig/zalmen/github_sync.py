"""
GitHub API helper for persisting TSV files across Streamlit Cloud redeploys.

Usage:
    from zalmen.github_sync import push_file_to_github
    push_file_to_github("Zylbercweig/organizations/org_alignment_review.tsv", local_path)

Performance note: editing a single cell (e.g. an org_type) rewrites and pushes
the *entire* TSV. The large alignment file (~2 MB) made every save a noticeable
freeze. Two synchronous-but-lighter optimisations here:
  - **SHA cache** — the GitHub Contents API needs the current blob SHA to update
    an existing file. We cache it (seeded from each PUT response) so we can skip
    the extra GET round-trip on subsequent saves. A 409 (stale SHA, e.g. from a
    parallel RA session) invalidates the cache and retries once with a fresh GET.
  - **Keep-alive session** — a module-level `requests.Session` reuses the TCP/TLS
    connection across saves, removing the handshake latency from each push.

The call remains synchronous and still returns True only after the commit lands,
so a successful return continues to mean "persisted to GitHub".
"""

from __future__ import annotations

import base64
import pathlib

import requests
import streamlit as st

# Reused across reruns within a single Streamlit process: keep-alive connection
# pool + last-known blob SHA per repo path (skips the GET round-trip).
_SESSION = requests.Session()
_SHA_CACHE: dict[str, str] = {}


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
            content_b64 = base64.b64encode(f.read()).decode()

        url = f"https://api.github.com/repos/{repo}/contents/{repo_path}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

        def _fetch_sha() -> str | None:
            resp = _SESSION.get(url, headers=headers, params={"ref": branch}, timeout=10)
            return resp.json().get("sha") if resp.ok else None

        def _put(sha: str | None):
            payload: dict = {
                "message": commit_message,
                "content": content_b64,
                "branch": branch,
            }
            if sha:
                payload["sha"] = sha
            return _SESSION.put(url, json=payload, headers=headers, timeout=15)

        # Use the cached SHA when we have one; otherwise fetch it once.
        sha = _SHA_CACHE.get(repo_path) or _fetch_sha()
        put_resp = _put(sha)

        # 409 = the SHA was stale (a parallel session pushed in between, or our
        # cache is out of date). Refetch the live SHA and retry exactly once.
        if put_resp.status_code == 409:
            sha = _fetch_sha()
            put_resp = _put(sha)

        if put_resp.ok:
            new_sha = (put_resp.json().get("content") or {}).get("sha")
            if new_sha:
                _SHA_CACHE[repo_path] = new_sha
            else:
                _SHA_CACHE.pop(repo_path, None)
            return True

        # On any other failure, drop the cached SHA so the next save re-fetches.
        _SHA_CACHE.pop(repo_path, None)
        return False

    except Exception:
        _SHA_CACHE.pop(repo_path, None)
        return False
