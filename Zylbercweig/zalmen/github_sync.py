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

Why the data has to go to the deployed branch at all: nothing here ever fetches.
Every view reads plain local paths inside the checkout, and Streamlit Cloud
rebuilds that checkout from the deployed branch on each restart — so a decision
that is not committed to THAT branch is gone the next time the server restarts.
That is also why the obvious cure for the redeploy churn (push the data to a
branch Cloud does not deploy) is not one: it would persist the saves and hide
them from the app at the same time.

What is left is to make each save cost ONE commit instead of one per file, which
is what `push_files_to_github` is for. The Contents API commits a single file per
call; the Git Data API can put several files in one commit, so an action that
writes N files restarts the server once rather than N times.
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


def push_files_to_github(
    files: list[tuple[str, pathlib.Path]], commit_message: str
) -> bool:
    """
    Commit SEVERAL files in ONE commit via the Git Data API.

    Each reviewer action that writes more than one TSV used to push once per
    file, and Streamlit Cloud restarts the server on every push — so a two-file
    save dropped every live session twice, which the RA experiences as the app
    jumping mid-work. One commit means one restart.

    Args:
        files:          (repo_path, local_path) pairs; empty list is a no-op.
        commit_message: Git commit message.

    Returns True only once the ref points at the new commit. On ANY failure it
    falls back to pushing the files one by one through the Contents API — worse
    for churn, but a save must never be lost to an optimisation.
    """
    if not files:
        return True
    if len(files) == 1:
        return push_file_to_github(files[0][0], files[0][1], commit_message)

    try:
        token = st.secrets.get("github_token", "")
        repo = st.secrets.get("github_repo", "")
        branch = st.secrets.get("github_branch", "main")
    except Exception:  # noqa: BLE001
        return False
    if not token or not repo:
        return False

    api = f"https://api.github.com/repos/{repo}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    def _fallback() -> bool:
        # Sequential Contents-API pushes: N commits, N restarts, but persisted.
        ok = True
        for repo_path, local_path in files:
            ok = push_file_to_github(repo_path, local_path, commit_message) and ok
        return ok

    try:
        payloads = []
        for repo_path, local_path in files:
            with open(local_path, "rb") as f:
                payloads.append((repo_path, base64.b64encode(f.read()).decode()))

        def _attempt() -> bool:
            # 1. where the branch is now
            r = _SESSION.get(f"{api}/git/ref/heads/{branch}", headers=headers, timeout=10)
            if not r.ok:
                return False
            head_sha = r.json()["object"]["sha"]
            r = _SESSION.get(f"{api}/git/commits/{head_sha}", headers=headers, timeout=10)
            if not r.ok:
                return False
            base_tree = r.json()["tree"]["sha"]

            # 2. a blob per file
            tree_entries = []
            for repo_path, content_b64 in payloads:
                r = _SESSION.post(
                    f"{api}/git/blobs", headers=headers, timeout=15,
                    json={"content": content_b64, "encoding": "base64"},
                )
                if not r.ok:
                    return False
                tree_entries.append({
                    "path": repo_path, "mode": "100644",
                    "type": "blob", "sha": r.json()["sha"],
                })

            # 3. one tree, 4. one commit, 5. move the ref
            r = _SESSION.post(f"{api}/git/trees", headers=headers, timeout=15,
                              json={"base_tree": base_tree, "tree": tree_entries})
            if not r.ok:
                return False
            new_tree = r.json()["sha"]
            r = _SESSION.post(f"{api}/git/commits", headers=headers, timeout=15,
                              json={"message": commit_message, "tree": new_tree,
                                    "parents": [head_sha]})
            if not r.ok:
                return False
            new_commit = r.json()["sha"]
            # force stays false: a non-fast-forward must fail loudly so the retry
            # rebuilds on top of whatever the other session just pushed.
            r = _SESSION.patch(f"{api}/git/refs/heads/{branch}", headers=headers,
                               timeout=15, json={"sha": new_commit, "force": False})
            return bool(r.ok)

        # One retry: a parallel RA session moving the branch between our read of
        # the ref and the PATCH is normal, not an error.
        ok = _attempt() or _attempt()
        if not ok:
            return _fallback()

        # These paths went out through the Git Data API, so the Contents-API SHA
        # cache no longer describes them.
        for repo_path, _ in files:
            _SHA_CACHE.pop(repo_path, None)
        return True

    except Exception:  # noqa: BLE001
        for repo_path, _ in files:
            _SHA_CACHE.pop(repo_path, None)
        try:
            return _fallback()
        except Exception:  # noqa: BLE001
            return False
