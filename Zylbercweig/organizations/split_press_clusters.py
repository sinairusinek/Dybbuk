#!/usr/bin/env python3
"""Split periodical clusters that mix different papers, by the lexicon entry each
mention comes from.

Source of truth: press_cluster_splits.tsv, one row per (cluster, entry heading):

    cluster_id  part  heading_contains  org_type  label  rationale

`part` is a small integer: every mention of `cluster_id` whose entry heading
contains `heading_contains` goes to that part. Part 1 is the default — any
mention no row names stays there — so only the minority papers need listing.
Each part becomes a child cluster `<cluster_id>_P01`, `_P02`, … (the live
`_P` partition convention, also used on top of `_Q` ids), with its own review
row in org_alignment_review.tsv built from the parent's, `org_type` from the
TSV (a troupe or society mixed into a newspaper cluster leaves as its own type).

Why the heading and not the sentence: in one cluster each lexicon entry speaks
of one paper, and the heading survives a re-extraction that rewrites sentences.
Same pattern as split_tsentral.py (2026-10-13), generalised.

The parent's links move with part 1: if the parent was ALIGNed to a db row,
part 1 inherits the alignment and core_db's linked_cluster_ids swaps the parent
for part 1; other parts start undecided, for apply_press_audit.py to align or
mint (press_audit_decisions.tsv names them by their child ids). The address
card's cluster list is updated the same way.

organizations_clustered.tsv and org_alignment_review.tsv are checked and
repaired INDEPENDENTLY (the app rewrites the latter and has reverted a split
there before — feedback_zalmen_stale_checkout_overwrites_core_db).

Idempotent. Backups .pre_press_split_<TS>. Dry-run by default.

Usage: python3 split_press_clusters.py [--apply]
"""
from __future__ import annotations

import csv
import io
import shutil
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

csv.field_size_limit(10 ** 9)

HERE = Path(__file__).resolve().parent
CLUSTERED = HERE / "organizations_clustered.tsv"
ALIGN = HERE / "org_alignment_review.tsv"
CORE_DB = HERE / "core_db.tsv"
ADDR = HERE / "org_addresses_review.tsv"
PLAN = HERE / "press_cluster_splits.tsv"
HEAD = "_ - heading"


def unpointed(s: str) -> str:
    # the corpus mixes precomposed פֿ (U+FB4E) with פ + rafe; compare unpointed
    import unicodedata
    return "".join(c for c in unicodedata.normalize("NFKD", s or "")
                   if not unicodedata.combining(c))


def read(path: Path):
    # newline="" keeps \r\n intact; read_text() would translate it and the
    # file would be rewritten LF (split_tsentral.py has that bug)
    with open(path, encoding="utf-8", newline="") as fh:
        raw = fh.read()
    nl = "\r\n" if "\r\n" in raw[:200000] else "\n"
    rd = csv.DictReader(io.StringIO(raw), delimiter="\t")
    return list(rd.fieldnames), list(rd), nl


def write(path: Path, fields, rows, nl):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fields, delimiter="\t",
                       lineterminator=nl, quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerows(rows)
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(buf.getvalue())


def pipes(v: str) -> list[str]:
    return [x.strip() for x in (v or "").split("|") if x.strip()]


def main() -> int:
    apply = "--apply" in sys.argv
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    _, plan, _ = read(PLAN)
    by_parent: dict[str, list[dict]] = defaultdict(list)
    for p in plan:
        by_parent[p["cluster_id"].strip()].append(p)

    cf, crows, cnl = read(CLUSTERED)
    af, arows, anl = read(ALIGN)
    kf, krows, knl = read(CORE_DB)
    df, drows, dnl = read(ADDR)
    changed = {"clustered": False, "align": False, "core": False, "addr": False}
    errors = 0

    for parent, prows in by_parent.items():
        child = lambda n: f"{parent}_P{int(n):02d}"  # noqa: E731
        parts = sorted({int(p["part"]) for p in prows} | {1})
        meta = {1: next((p for p in prows if int(p["part"]) == 1), None)}
        for p in prows:
            meta.setdefault(int(p["part"]), p)

        # ---- organizations_clustered.tsv ----
        mine = [r for r in crows if r["cluster_id"].strip() == parent]
        done_src = not mine and any(r["cluster_id"].strip() == child(1) for r in crows)
        if mine:
            assign: dict[int, list[dict]] = defaultdict(list)
            used = set()
            for r in mine:
                part = 1
                for p in prows:
                    if p["heading_contains"].strip() and \
                            unpointed(p["heading_contains"].strip()) in unpointed(r[HEAD]):
                        part = int(p["part"])
                        used.add(id(p))
                        break
                assign[part].append(r)
            for p in prows:
                if id(p) not in used and p["heading_contains"].strip():
                    print(f"ERROR {parent}: no mention under heading containing "
                          f"«{p['heading_contains']}»", file=sys.stderr)
                    errors += 1
            if any(len(assign[n]) == 0 for n in parts):
                print(f"ERROR {parent}: an empty part {[n for n in parts if not assign[n]]}",
                      file=sys.stderr)
                errors += 1
                continue
            for n in parts:
                for r in assign[n]:
                    r["cluster_id"] = child(n)
                    r["cluster_size"] = str(len(assign[n]))
            changed["clustered"] = True
            print(f"{parent}: {len(mine)} mentions -> "
                  + ", ".join(f"{child(n)}={len(assign[n])}" for n in parts))
        elif not done_src:
            print(f"ERROR {parent}: not in {CLUSTERED.name}", file=sys.stderr)
            errors += 1
            continue
        sizes = defaultdict(int)
        for r in crows:
            if r["cluster_id"].strip().startswith(parent + "_P"):
                sizes[r["cluster_id"].strip()] += 1

        # ---- org_alignment_review.tsv ----
        prow = next((r for r in arows if r["cluster_id"].strip() == parent), None)
        if prow is not None:
            idx = arows.index(prow)
            new = []
            for n in parts:
                m = meta.get(n) or {}
                row = dict(prow)
                row["cluster_id"] = child(n)
                row["cluster_size"] = str(sizes.get(child(n), 0))
                if (m.get("org_type") or "").strip():
                    row["org_type"] = m["org_type"].strip()
                if n != 1:
                    for c in ("decision", "aligned_db_id", "reviewer", "reviewed_at"):
                        row[c] = ""
                note = (f"split from {parent} {datetime.now():%Y-%m-%d} (press audit): "
                        f"{(m.get('label') or '').strip()} — {(m.get('rationale') or '').strip()}")
                prev = (prow.get("reviewer_notes") or "").strip()
                row["reviewer_notes"] = (prev + " | " if prev else "") + note
                new.append(row)
            arows[idx:idx + 1] = new
            changed["align"] = True
        elif not any(r["cluster_id"].strip() == child(1) for r in arows):
            print(f"ERROR {parent}: not in {ALIGN.name}", file=sys.stderr)
            errors += 1

        # ---- core_db + address cards: the parent's link passes to part 1 ----
        for rows, key in ((krows, "core"), (drows, "addr")):
            for r in rows:
                ids = pipes(r.get("linked_cluster_ids", ""))
                if parent in ids:
                    r["linked_cluster_ids"] = " | ".join(child(1) if c == parent else c for c in ids)
                    changed[key] = True
                    if key == "core":
                        print(f"   core_db {r['db_id']}: {parent} -> {child(1)}")

    if errors:
        print(f"\n{errors} error(s); nothing written.", file=sys.stderr)
        return 2
    if not apply:
        print("\nDRY RUN — nothing written. Re-run with --apply.")
        return 0
    for key, path, fields, rows, nl in (("clustered", CLUSTERED, cf, crows, cnl),
                                        ("align", ALIGN, af, arows, anl),
                                        ("core", CORE_DB, kf, krows, knl),
                                        ("addr", ADDR, df, drows, dnl)):
        if changed[key]:
            shutil.copy2(path, path.with_suffix(f".tsv.pre_press_split_{ts}"))
            write(path, fields, rows, nl)
    print(f"\nWROTE {[k for k, v in changed.items() if v]} (backups .pre_press_split_{ts})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
