"""A1 — Seed the play registry from lexicon-derived data only.

Reads `created_expressions` for the requested author db_ids from
people/people_db.tsv, mints one play node per (author, title) claim, dedups
within each author by normalized-title identity, and flags cross-author
title collisions as disputed (Lateiner and Hurwitz wrote rival same-titled
plays — a shared title is NOT evidence of a shared work).

Outputs (on --execute): plays_db.tsv, play_title_collisions.tsv.
df_corpus / ambiguity_flag are left empty here; find_title_hits.py fills
them during the corpus sweep.

Usage:
    python3.11 build_plays_db.py               # dry-run: stats only
    python3.11 build_plays_db.py --execute
    python3.11 build_plays_db.py --author-db-ids 683,684 --execute
"""
from __future__ import annotations

import argparse
from collections import defaultdict

import plays_common as pc

PLAYS_FIELDS = [
    "play_id", "title_yiddish", "title_segments_norm", "alt_titles",
    "author_db_id", "author_heading", "attribution_status", "same_title_as",
    "df_corpus", "ambiguity_flag", "source", "notes",
]
COLLISION_FIELDS = [
    "collision_key", "play_id_a", "author_a", "title_a",
    "play_id_b", "author_b", "title_b", "basis", "resolution", "notes",
]


def load_author_rows(author_db_ids: list[str]) -> list[dict]:
    rows = [r for r in pc.read_tsv(pc.PEOPLE_DB_TSV) if r.get("db_id") in author_db_ids]
    missing = set(author_db_ids) - {r["db_id"] for r in rows}
    if missing:
        raise SystemExit(f"author db_id(s) not found in people_db.tsv: {sorted(missing)}")
    return rows


def build_registry(author_rows: list[dict]) -> tuple[list[dict], list[dict]]:
    plays: list[dict] = []
    dropped_dups: list[tuple[str, str, str]] = []  # (author, kept_title, dropped_title)

    for author in sorted(author_rows, key=lambda r: r["db_id"]):
        seen_norm: dict[str, dict] = {}
        titles = [t.strip() for t in (author.get("created_expressions") or "").split(";") if t.strip()]
        for title in sorted(titles):
            norm = pc.norm_yiddish(title)
            if not norm:
                continue
            if norm in seen_norm:
                kept = seen_norm[norm]
                dropped_dups.append((author["db_id"], kept["title_yiddish"], title))
                if title not in kept["alt_titles"]:
                    kept["alt_titles"] = "; ".join(x for x in [kept["alt_titles"], title] if x)
                continue
            row = {
                "play_id": "",  # minted after full collection for stable order
                "title_yiddish": title,
                "title_segments_norm": "|".join(pc.title_segments(title)),
                "alt_titles": "",
                "author_db_id": author["db_id"],
                "author_heading": author.get("hebname", ""),
                "attribution_status": "single",
                "same_title_as": "",
                "df_corpus": "",
                "ambiguity_flag": "",
                "source": "people_db.created_expressions",
                "notes": "",
            }
            seen_norm[norm] = row
            plays.append(row)

    for i, row in enumerate(plays, 1):
        row["play_id"] = f"PL-{i:04d}"

    # Cross-author collisions: identical normalized full title or main (first)
    # segment marks both nodes disputed; a shared SUBTITLE segment under
    # different main titles is only recorded in the audit file. Both nodes
    # always stay; they only get cross-referenced.
    collisions: list[dict] = []
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in plays:
        segs = row["title_segments_norm"].split("|") if row["title_segments_norm"] else []
        keys = {("full", pc.norm_yiddish(row["title_yiddish"]))}
        if segs:
            keys.add(("seg", segs[0]))
            for sub in segs[1:]:
                keys.add(("subtitle", sub))
        for key in keys:
            by_key[key].append(row)

    seen_pairs: set[tuple[str, str]] = set()
    for (basis, key), group in sorted(by_key.items()):
        authors = {r["author_db_id"] for r in group}
        if len(authors) < 2:
            continue
        for a in group:
            for b in group:
                if a["author_db_id"] >= b["author_db_id"]:
                    continue
                pair = (a["play_id"], b["play_id"])
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                for r in (a, b):
                    if basis in ("full", "seg"):
                        r["attribution_status"] = "disputed"
                    other = b if r is a else a
                    ids = [x for x in r["same_title_as"].split("|") if x]
                    if other["play_id"] not in ids:
                        ids.append(other["play_id"])
                    r["same_title_as"] = "|".join(ids)
                collisions.append({
                    "collision_key": key,
                    "play_id_a": a["play_id"], "author_a": a["author_db_id"],
                    "title_a": a["title_yiddish"],
                    "play_id_b": b["play_id"], "author_b": b["author_db_id"],
                    "title_b": b["title_yiddish"],
                    "basis": basis, "resolution": "", "notes": "",
                })

    if dropped_dups:
        print(f"deduped within-author (kept as alt_titles): {len(dropped_dups)}")
        for author, kept, dropped in dropped_dups:
            print(f"  [{author}] {dropped!r} -> {kept!r}")
    return plays, collisions


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--author-db-ids", default=",".join(pc.DEFAULT_AUTHOR_DB_IDS))
    ap.add_argument("--execute", action="store_true", help="write TSVs (default: dry-run)")
    args = ap.parse_args()

    author_db_ids = [x.strip() for x in args.author_db_ids.split(",") if x.strip()]
    author_rows = load_author_rows(author_db_ids)
    plays, collisions = build_registry(author_rows)

    per_author = defaultdict(int)
    for r in plays:
        per_author[r["author_db_id"]] += 1
    print(f"plays: {len(plays)} total  " +
          "  ".join(f"[{a}]={n}" for a, n in sorted(per_author.items())))
    print(f"disputed (cross-author title collisions): "
          f"{sum(1 for r in plays if r['attribution_status'] == 'disputed')} plays, "
          f"{len(collisions)} pairs")
    n_segs = sum(len(r["title_segments_norm"].split("|")) for r in plays if r["title_segments_norm"])
    print(f"normalized segments indexed: {n_segs}")

    if not args.execute:
        print("\ndry-run — pass --execute to write plays_db.tsv / play_title_collisions.tsv")
        return

    # Preserve any prior manual columns (resolution/notes) on re-run.
    prior = {r["collision_key"] + r["play_id_a"] + r["play_id_b"]: r
             for r in pc.read_tsv(pc.COLLISIONS_TSV)}
    for c in collisions:
        old = prior.get(c["collision_key"] + c["play_id_a"] + c["play_id_b"])
        if old:
            c["resolution"], c["notes"] = old.get("resolution", ""), old.get("notes", "")
    prior_plays = {r["play_id"]: r for r in pc.load_plays_db()}
    for r in plays:
        old = prior_plays.get(r["play_id"])
        if old:
            r["notes"] = old.get("notes", "") or r["notes"]
            r["df_corpus"] = old.get("df_corpus", "")
            r["ambiguity_flag"] = old.get("ambiguity_flag", "")

    pc.write_tsv(pc.PLAYS_DB_TSV, plays, PLAYS_FIELDS)
    pc.write_tsv(pc.COLLISIONS_TSV, collisions, COLLISION_FIELDS)
    print(f"wrote {pc.PLAYS_DB_TSV} ({len(plays)}) and {pc.COLLISIONS_TSV} ({len(collisions)})")


if __name__ == "__main__":
    main()
