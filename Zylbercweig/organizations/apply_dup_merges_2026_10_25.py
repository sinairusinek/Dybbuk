"""Merge the duplicates surfaced by the Trade Union recheck (Sinai, 2026-10-25).

See apply_trade_union_reclass_2026_10_25.py for the retyping. Two entities
were each minted more than once; both pairs are partly double-mints of the
same cluster (2736/2737 both carry ORG-C05304, 3001/3002 both ORG-C03323):

  2413  actors' union Local 18   <- 2736, 2737, 3000
  3061  Zachęta (Warsaw)         <- 3001, 3002

Each merge does what the app's _merge_db_identities does, except the
core_db row is kept as deprecated=true + merged_into (the script-side
convention; reversible, and the id stays above the mint high-water mark):

  core_db.tsv                 clusters move to the keeper; spelling -> name_variants
  org_alignment_review.tsv    aligned_db_id loser -> keeper
  org_addresses_review.tsv    clusters union into keeper's card; loser card dropped;
                              parent_db_id loser -> keeper
  graph/person_org_edges.tsv  db_id loser -> keeper (derived; saves a rebuild,
                              which would wipe the eponym edges)
  activity_log.tsv            one MERGE_INTO row per loser, stamped --reviewer

Idempotent: a second --apply changes and logs nothing.

Usage:
    python3 Zylbercweig/organizations/apply_dup_merges_2026_10_25.py [--apply --reviewer Sinai]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import io
import json
import re
import sys
import unicodedata
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
ALIGN = HERE / "org_alignment_review.tsv"
ADDR = HERE / "org_addresses_review.tsv"
EDGES = HERE / "graph" / "person_org_edges.tsv"
ACTIVITY_LOG = HERE / "activity_log.tsv"

LOG_COLUMNS = ("ts", "reviewer", "view", "action", "target_id", "decision",
               "note", "extra")

# loser -> (keeper, why)
MERGES: dict[str, tuple[str, str]] = {
    "2736": ("2413", "actors' union Local 18; 2736/2737 double-minted ORG-C05304"),
    "2737": ("2413", "actors' union Local 18; 2736/2737 double-minted ORG-C05304"),
    "3000": ("2413", "'Local 18' — the text calls it the aktyorn-yunye"),
    "3001": ("3061", "Zachęta; 3001/3002 double-minted ORG-C03323"),
    "3002": ("3061", "Zachęta; 3001/3002 double-minted ORG-C03323"),
}

POINTS = re.compile(r"[֑-ׇ]")


def norm(s: str) -> str:
    return POINTS.sub("", unicodedata.normalize("NFKD", s or "")).strip()


def pipe_list(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def add_pipe(row: dict, col: str, vals: list[str]) -> bool:
    cur = pipe_list(row.get(col, ""))
    new = [v for v in vals if v not in cur]
    if new:
        row[col] = " | ".join(cur + new)
    return bool(new)


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    # keep each file's own terminator: core_db/addresses/alignment are CRLF,
    # person_org_edges is LF
    term = "\r\n" if b"\r\n" in p.read_bytes()[:4096] else "\n"
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t", lineterminator=term)
        w.writeheader()
        w.writerows(rows)


def log_rows(entries: list[dict]) -> int:
    """Append to activity_log.tsv with CRLF, as the app's tail does."""
    if not entries:
        return 0
    existing = ACTIVITY_LOG.read_bytes()
    header = existing.split(b"\n", 1)[0].rstrip(b"\r").decode("utf-8").split("\t")
    if tuple(header) != LOG_COLUMNS:
        raise SystemExit(f"{ACTIVITY_LOG.name} header is {header}")
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(LOG_COLUMNS), delimiter="\t",
                       lineterminator="\r\n")
    w.writerows(entries)
    with ACTIVITY_LOG.open("ab") as f:
        if not existing.endswith(b"\n"):
            f.write(b"\r\n")
        f.write(buf.getvalue().encode("utf-8"))
    return len(entries)


def main(apply: bool, reviewer: str) -> None:
    core_f, core = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in core}
    for lo, (kp, _) in MERGES.items():
        if lo not in by_id or kp not in by_id:
            raise SystemExit(f"missing db_id {lo if lo not in by_id else kp}")
        if by_id[kp].get("deprecated", "").strip() in ("true", "1"):
            raise SystemExit(f"keeper {kp} is itself deprecated")

    log: list[dict] = []
    now = _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # 1. core_db
    for lo, (kp, why) in MERGES.items():
        lrow, krow = by_id[lo], by_id[kp]
        if lrow.get("merged_into", "").strip() == kp:
            continue
        moved = pipe_list(lrow["linked_cluster_ids"])
        add_pipe(krow, "linked_cluster_ids", moved)
        lname = (lrow["name_yiddish"] or lrow["name"]).strip()
        known = [krow["name"], krow["name_yiddish"]] + pipe_list(krow.get("name_variants", ""))
        if lname and not any(norm(v) == norm(lname) for v in known):
            add_pipe(krow, "name_variants", [lname])
        lrow["merged_into"] = kp
        lrow["deprecated"] = "true"
        lrow["linked_cluster_ids"] = ""
        print(f"  core_db  {lo} -> {kp}  clusters {moved}  — {why}")
        log.append({
            "ts": now, "reviewer": reviewer, "view": "org_addresses",
            "action": "db_merge", "target_id": kp, "decision": "MERGE_INTO",
            "note": f"merged db_id {lo} into {kp}: {why}",
            "extra": json.dumps({"loser_id": lo, "survivor_id": kp,
                                 "clusters_moved": moved,
                                 "source": Path(__file__).name}, ensure_ascii=False),
        })

    # 2. alignments
    al_f, al = _load(ALIGN)
    n_al = 0
    for r in al:
        lo = r.get("aligned_db_id", "").strip()
        if lo in MERGES:
            r["aligned_db_id"] = MERGES[lo][0]
            n_al += 1
            print(f"  align    {r['cluster_id']}: {lo} -> {MERGES[lo][0]}")

    # 3. address cards
    ad_f, ad = _load(ADDR)
    ad_by = {r["db_id"].strip(): r for r in ad}
    n_card = n_sub = 0
    for lo, (kp, _) in MERGES.items():
        lcard, kcard = ad_by.get(lo), ad_by.get(kp)
        if lcard is None:
            continue
        if kcard is not None:
            add_pipe(kcard, "linked_cluster_ids", pipe_list(lcard["linked_cluster_ids"]))
            for c in ("confirmed_settlement", "confirmed_settlement_yiddish",
                      "confirmed_address", "confirmed_address_romanized", "lat", "lon"):
                if not (kcard.get(c) or "").strip() and (lcard.get(c) or "").strip():
                    kcard[c] = lcard[c]
            lnote = (lcard.get("reviewer_notes") or "").strip()
            if lnote:
                tag = f"[merged from db_id {lo}] {lnote}"
                knote = (kcard.get("reviewer_notes") or "").strip()
                kcard["reviewer_notes"] = f"{knote}\n---\n{tag}" if knote else tag
        n_card += 1
    if n_card:
        kept = []
        for r in ad:
            d = r["db_id"].strip()
            if d in MERGES:
                print(f"  address  card {d} dropped into {MERGES[d][0]}")
                continue
            if r.get("parent_db_id", "").strip() in MERGES:
                r["parent_db_id"] = MERGES[r["parent_db_id"].strip()][0]
                n_sub += 1
            kept.append(r)
        ad = kept

    # 4. person_org_edges (derived)
    ed_f, ed = _load(EDGES)
    n_ed = 0
    for r in ed:
        lo = r.get("db_id", "").strip()
        if lo in MERGES:
            r["db_id"] = MERGES[lo][0]
            n_ed += 1

    print(f"\nmerges logged: {len(log)}  alignments: {n_al}  cards dropped: {n_card}"
          f"  sub-entries: {n_sub}  edges: {n_ed}")
    if not apply:
        print("(dry-run; pass --apply --reviewer NAME to write)")
        return
    _dump(CORE_DB, core_f, core)
    if n_al:
        _dump(ALIGN, al_f, al)
    if n_card:
        _dump(ADDR, ad_f, ad)
    if n_ed:
        _dump(EDGES, ed_f, ed)
    print(f"logged {log_rows(log)} rows as {reviewer!r}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--reviewer", default="")
    a = ap.parse_args()
    if a.apply and not a.reviewer.strip():
        ap.error("--apply requires --reviewer")
    main(a.apply, a.reviewer.strip())
