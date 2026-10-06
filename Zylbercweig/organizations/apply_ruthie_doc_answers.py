"""Apply Ruthie's answers from the troupe-questions doc (answered 2026-10-04).

Source of truth: ruthie_doc_answers_2026-10-04.tsv, transcribed from the doc
she filled in (https://claude.ai/code/artifact/68e97805-39e8-49c7-bb23-a624f63cc8bb).

What this does, per decision:
  LINK   a bundle part -> that part's mention(s) attach to the EXISTING org she
         named; the part mints nothing. The bundle row's cluster is added to the
         target's linked_cluster_ids.
  CREATE a bundle part -> mint a new core_db row named "<part>ס טרופּע" carrying
         the bundle's cluster.
  REMOVE -> mark the row deprecated (NOT a hard delete; see below).
  HOLD   -> never written; reported for the follow-up list.

Every bundle row that is fully dispatched (all its parts LINK/CREATE) is itself
marked deprecated, because the plural heading is not an organization.

DELIBERATELY CONSERVATIVE on two points:
  1. REMOVE sets deprecated=1 rather than deleting the row. Her comments say
     these are not organizations, but the rows carry cluster links that other
     tools read, and feedback_core_db_link_authority says those links have no
     single authority. Deprecating is reversible; deleting is not.
  2. A minted row gets the bundle's cluster id in linked_cluster_ids, but this
     script does NOT touch org_alignment_review.tsv's aligned_db_id. One cluster
     cannot point at N new orgs, and picking one would be a guess. That
     re-alignment is the next step and needs its own decision.

Yiddish name matching: normalize NFKD *then* strip points. The corpus uses the
precomposed ligature U+FB4E (פֿ) where a typed string uses פ + rafe; stripping
points alone cannot match them and silently finds nothing.

Backup to core_db.tsv.pre_ruthie_doc_<TS>. Dry-run by default.

Usage: python3 apply_ruthie_doc_answers.py [--apply]
"""
from __future__ import annotations

import argparse
import csv
import datetime as _dt
import re
import shutil
import unicodedata
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
ANSWERS = HERE / "ruthie_doc_answers_2026-10-04.tsv"

POINTS = re.compile(r"[֑-ׇ]")


def norm(s: str) -> str:
    """NFKD first, then strip points — see module docstring."""
    return POINTS.sub("", unicodedata.normalize("NFKD", s or "")).strip()


FINAL_FORMS = {"ן": "נ", "ם": "מ", "ך": "כ", "ף": "פ", "ץ": "צ"}


def troupe_name(part: str) -> str:
    """"<surname>ס טרופּע", undoing a final letter form before the possessive ס.

    Yiddish final forms only stand at a word's end, so גאָלדפאַדען + ס is
    גאָלדפאַדענס, not גאָלדפאַדעןס. A name ending in a period (an initial, e.g.
    "ד. היידעמאַקו") takes the possessive on the last word only.
    """
    p = part.strip()
    if p and p[-1] in FINAL_FORMS:
        p = p[:-1] + FINAL_FORMS[p[-1]]
    return f"{p}ס טרופּע"


def _load(p: Path) -> tuple[list[str], list[dict]]:
    with p.open(newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        return list(rd.fieldnames or []), list(rd)


def _dump(p: Path, fields: list[str], rows: list[dict]) -> None:
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows)


def add_cluster(row: dict, cid: str) -> bool:
    cur = [c.strip() for c in (row.get("linked_cluster_ids") or "").split("|") if c.strip()]
    if cid in cur:
        return False
    row["linked_cluster_ids"] = " | ".join(cur + [cid])
    return True


def main(apply: bool) -> None:
    _, answers = _load(ANSWERS)
    fields, rows = _load(CORE_DB)
    by_id = {r["db_id"].strip(): r for r in rows}

    next_id = max(int(r["db_id"]) for r in rows if r["db_id"].strip().isdigit()) + 1

    links: list[tuple[str, str, str, str]] = []   # bundle, part, target, cid
    creates: list[tuple[str, str, str]] = []      # bundle, part, cid
    removes: list[tuple[str, str]] = []           # db_id, why
    holds: list[tuple[str, str]] = []
    problems: list[str] = []
    already: list[str] = []
    merges: list[tuple[str, str, str]] = []
    noops: list[tuple[str, str, str]] = []

    parts_by_bundle: dict[str, list[str]] = {}

    for a in answers:
        q, kind, dec = a["q"], a["kind"], a["decision"].strip().upper()
        bundle = a["bundle_db_id"].strip()
        part = a["part"].strip()

        if dec == "HOLD":
            holds.append((q, a["note"]))
            continue
        if dec in ("LEAVE", "NOOP"):
            noops.append((q, dec, a["note"]))
            continue
        if dec == "MERGE":
            src, tgt = bundle, a["target_db_id"].strip()
            if src not in by_id or tgt not in by_id:
                problems.append(f"q{q}: MERGE db{src} -> db{tgt}, one of them not in core_db")
                continue
            merges.append((src, tgt, a["note"]))
            continue

        if kind == "spelling":
            problems.append(f"q{q}: spelling row with decision {dec}, unhandled")
            continue

        if kind == "bundle_part":
            brow = by_id.get(bundle)
            if brow is None:
                problems.append(f"q{q}: bundle db{bundle} not in core_db")
                continue
            cids = [c.strip() for c in (brow["linked_cluster_ids"] or "").split("|") if c.strip()]
            if len(cids) != 1:
                problems.append(f"q{q}: bundle db{bundle} has {len(cids)} clusters, expected 1")
                continue
            parts_by_bundle.setdefault(bundle, []).append(q)
            if dec == "LINK":
                tgt = a["target_db_id"].strip()
                if tgt not in by_id:
                    problems.append(f"q{q}: LINK target db{tgt} not in core_db")
                    continue
                links.append((bundle, part, tgt, cids[0]))
            elif dec == "CREATE":
                # Idempotence guard: a previous --apply already minted this org.
                want = norm(troupe_name(part))
                if any(norm(r.get("name_yiddish", "")) == want for r in rows):
                    already.append(f"q{q}: {troupe_name(part)} already exists")
                    continue
                creates.append((bundle, part, cids[0]))
        elif kind == "remove" and dec == "REMOVE":
            if bundle not in by_id:
                problems.append(f"q{q}: remove target db{bundle} not in core_db")
                continue
            removes.append((bundle, a["note"]))
        # db888_fischer / db774_breitman / serial_comma_rule carry no core_db edit here

    # A bundle is retired only when EVERY one of its parts was dispatched.
    expected = {}
    for a in answers:
        if a["kind"] == "bundle_part":
            expected.setdefault(a["bundle_db_id"].strip(), []).append(a["decision"].strip().upper())
    retire = [b for b, decs in expected.items() if decs and all(d in ("LINK", "CREATE") for d in decs)]
    kept = {b: decs for b, decs in expected.items() if b not in retire}

    if already:
        print(f"already applied, skipping ({len(already)}):")
        for a_ in already:
            print(f"  {a_}")
        print()
    print(f"LINK   {len(links)}   CREATE {len(creates)}   REMOVE {len(removes)}   HOLD {len(holds)}")
    print(f"\nBundle rows to retire (all parts dispatched): {sorted(retire, key=int)}")
    print(f"Bundle rows KEPT (a part is on hold): "
          f"{ {b: decs.count('HOLD') for b, decs in kept.items()} }")

    print("\nLINK — part's cluster joins an existing org:")
    for b, part, tgt, cid in links:
        t = by_id[tgt]
        print(f"  db{b} {part!r} -> db{tgt} {(t['name_yiddish'] or t['name'])!r}  (+{cid})")

    print("\nCREATE — new orgs:")
    for i, (b, part, cid) in enumerate(creates):
        print(f"  db{next_id + i}  {troupe_name(part)}   from db{b}  ({cid})")

    print("\nMERGE — fold source into target (merged_into + deprecated):")
    for src, tgt, why in merges:
        srow, trow = by_id[src], by_id[tgt]
        scl = [c.strip() for c in (srow["linked_cluster_ids"] or "").split("|") if c.strip()]
        print(f"  db{src} {(srow['name_yiddish'] or srow['name'])!r}")
        print(f"    -> db{tgt} {(trow['name_yiddish'] or trow['name'])!r}  moving {len(scl)} cluster(s): {scl}")
        print(f"    {why}")

    print(f"\nLEAVE / NOOP — recorded, no edit ({len(noops)}):")
    for q, dec, why in noops:
        print(f"  q{q} [{dec}]: {why}")

    print("\nREMOVE — set deprecated=1 (reversible, not deleted):")
    for db, why in removes:
        r = by_id[db]
        print(f"  db{db} {(r['name_yiddish'] or r['name'])!r} — {why}")

    print(f"\nHOLD — not applied, needs an answer ({len(holds)}):")
    for q, why in holds:
        print(f"  q{q}: {why}")

    if problems:
        print(f"\n*** PROBLEMS ({len(problems)}) — nothing will be written:")
        for p in problems:
            print(f"  {p}")
        return

    if not apply:
        print("\n(dry-run; pass --apply to write)")
        return

    ts = _dt.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    shutil.copy2(CORE_DB, CORE_DB.with_suffix(f".tsv.pre_ruthie_doc_{ts}"))

    if "deprecated" not in fields:
        raise SystemExit("core_db.tsv has no 'deprecated' column")

    n_link = 0
    for _b, _part, tgt, cid in links:
        if add_cluster(by_id[tgt], cid):
            n_link += 1

    for i, (b, part, cid) in enumerate(creates):
        new = {f: "" for f in fields}
        new["db_id"] = str(next_id + i)
        name = troupe_name(part)
        new["name"] = name
        new["name_yiddish"] = name
        new["org_type"] = by_id[b]["org_type"]
        new["linked_cluster_ids"] = cid
        rows.append(new)

    for src, tgt, _why in merges:
        srow, trow = by_id[src], by_id[tgt]
        for c in [x.strip() for x in (srow["linked_cluster_ids"] or "").split("|") if x.strip()]:
            add_cluster(trow, c)
        # carry the source's spelling onto the target as a variant
        sname = (srow["name_yiddish"] or srow["name"]).strip()
        if sname:
            cur = [v.strip() for v in (trow.get("name_variants") or "").split("|") if v.strip()]
            if not any(norm(v) == norm(sname) for v in cur):
                trow["name_variants"] = " | ".join(cur + [sname])
        srow["merged_into"] = tgt
        srow["deprecated"] = "true"   # matches the 53 existing merged rows
        srow["linked_cluster_ids"] = ""

    for db, _why in removes:
        by_id[db]["deprecated"] = "1"
    for b in retire:
        by_id[b]["deprecated"] = "1"

    _dump(CORE_DB, fields, rows)
    print(f"\nwrote: {n_link} cluster links, {len(creates)} new orgs, "
          f"{len(merges)} merged, {len(removes)} removed, "
          f"{len(retire)} bundle rows retired")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    main(ap.parse_args().apply)
