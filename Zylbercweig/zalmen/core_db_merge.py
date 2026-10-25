"""Three-way merge for core_db.tsv saves.

Every view saves core_db by rewriting the whole file from its own copy. When
that copy is older than GitHub — the Cloud checkout lags a push, or a script
updated the file offline — the save silently reverts everything newer. On
2026-10-25 one org_type edit reverted 16 Theatre retypes pushed ten minutes
earlier; on 2026-10-13 a save dropped 45 rows and 82 decisions.

So a save carries only what THIS session changed: the delta between the rows
it loaded (`base`, the local file) and the rows it is saving (`mine`), replayed
cell by cell onto the latest GitHub copy (`remote`). Pure functions, no
Streamlit, so the merge is testable.
"""
from __future__ import annotations

import csv
import io


def parse_tsv(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    r = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""), delimiter="\t")
    return list(r.fieldnames or []), list(r)


def _key(row: dict[str, str]) -> str:
    return (row.get("db_id") or "").strip()


def merge_core_db(
    base: list[dict[str, str]],
    mine: list[dict[str, str]],
    remote_headers: list[str],
    remote: list[dict[str, str]],
    mine_headers: list[str],
) -> tuple[list[str], list[dict[str, str]], list[str]]:
    """Replay base→mine onto remote. Returns (headers, rows, conflicts).

    - a cell this session changed is written onto remote's row;
      cells it did not touch keep remote's value;
    - a row this session added is appended (if remote already has that db_id
      with other content, this session's row wins and the id is reported in
      `conflicts` — two sessions minted the same id);
    - a row this session removed (merge cards) is removed from remote.
    Row order follows remote; additions go at the end.
    """
    headers = list(remote_headers)
    for h in mine_headers:
        if h not in headers:
            headers.append(h)

    base_by = {_key(r): r for r in base if _key(r)}
    mine_by = {_key(r): r for r in mine if _key(r)}
    out = [dict(r) for r in remote]
    out_by = {_key(r): r for r in out if _key(r)}
    conflicts: list[str] = []

    for k, m in mine_by.items():
        b = base_by.get(k)
        if b is None:
            existing = out_by.get(k)
            if existing is None:
                row = dict(m)
                out.append(row)
                out_by[k] = row
            elif any((existing.get(h) or "") != (m.get(h) or "") for h in headers):
                conflicts.append(k)
                existing.clear()
                existing.update(m)
            continue
        target = out_by.get(k)
        if target is None:
            continue  # removed upstream since this session loaded it
        for h in set(m) | set(b):
            if (m.get(h) or "") != (b.get(h) or ""):
                target[h] = m.get(h) or ""

    removed = {k for k in base_by if k not in mine_by}
    if removed:
        out = [r for r in out if _key(r) not in removed]

    for r in out:
        for h in headers:
            r.setdefault(h, "")
    return headers, out, conflicts
