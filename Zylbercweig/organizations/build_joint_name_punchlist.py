"""Flag rows whose NAME lists several people, for a one-by-one human read.

Ruthie finished the troupe tags (683/683) and her comments surfaced a class
tagging cannot express: an entry whose name enumerates several people. Two
different things look alike:

  a CATALOGUE BUNDLE — a Zylbercweig index heading listing several impresarios
    ("די טרופּעס פֿון X, Y און Z"). Not an organization. Those carry a PLURAL
    head-noun, were handled in bundle_resolve_candidates.tsv, and are done.

  a JOINT COMPANY — one real company billed under several names
    ("די טריאָ איז באַשטאַנען פֿון אַ באַס, אַ באַריטאָן און אַ טענאָר").
    Splitting one of these would be wrong — see feedback_duo_surnames_not_variants.

The remaining rows are serial lists with NO plural head-noun, so the name alone
cannot tell them apart. Asked in the questions doc how to handle them, Ruthie
chose "go through them one by one" over any blanket rule — so this builds the
queue rather than deciding anything.

Output: db_joint_name_punchlist.tsv, for the DB Audit view's "Joint names"
section. Decisions -> db_joint_name_decisions.tsv, keyed by db_id (the question
is about the ROW, not a cluster, unlike the other audit sections).

THE SENTENCE IS THE EVIDENCE, so it travels with the row: "די טריאָ איז
באַשטאַנען..." settles db1062 on sight, and a reviewer should not have to go
find it. mentions_json carries one entry per mention.

Deliberately EXCLUDED, with the reason recorded in the output:
  - rows with a plural head-noun (handled as bundles already)
  - rows with no cluster links at all — no mentions means nothing to judge;
    that is an orphan-row problem, not this question
  - rows whose org_type is not a performing type (an address-style name like
    "יידישער ראָדיקאָלער שולע, אַרבעטער רינג, בראנטש נומ' 3" matches a comma
    sweep but is a school branch)

Re-runnable: same input -> same output. Decisions are keyed separately and are
never touched here.

Usage: python3 build_joint_name_punchlist.py [--write]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import unicodedata
from pathlib import Path

csv.field_size_limit(10**9)

HERE = Path(__file__).resolve().parent
CORE_DB = HERE / "core_db.tsv"
CLUSTERED = HERE / "organizations_clustered.tsv"
OUT = HERE / "db_joint_name_punchlist.tsv"

COL_CID = "cluster_id"
COL_HEAD = "_ - heading"
COL_ORG = "clustered organization"
COL_SENT = "_ - organizations - _ - relations - _ - original_sentence"
COL_SETTLE = "_ - organizations - _ - locations - _ - settlement"
COL_DATE = "_ - organizations - _ - relations - _ - date_start - date"

POINTS = re.compile(r"[֑-ׇ]")

# Performing-org types only. A comma in a school or union name is usually
# address-style, not a list of people.
PERFORMING = {
    "traveling company", "company on tour", "theatre", "amateur",
    "kleinkunst", "musical organization", "non-yiddish theatre", "circus",
}


def norm(s: str) -> str:
    """NFKD first, THEN strip points: the corpus uses precomposed ligatures
    (U+FB4E פֿ) where a typed string has פ + rafe. Stripping points alone
    cannot match them and silently finds nothing."""
    return POINTS.sub("", unicodedata.normalize("NFKD", s or "")).strip()


PLURAL = re.compile("|".join(norm(w) for w in
                             ("טרופּעס", "קאָמפּאַניעס", "טעאַטערס", "טעאַטערן")))
AND_RE = re.compile(norm(r"און"))


def load(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def is_serial_list(name: str) -> bool:
    """Name enumerates 2+ people: 2+ commas, or a comma plus 'און'."""
    n = norm(name)
    ncom = n.count(",")
    return ncom >= 2 or (ncom >= 1 and bool(AND_RE.search(n)))


def main(write: bool) -> None:
    core = load(CORE_DB)
    clustered = load(CLUSTERED)
    ment_by_cid: dict[str, list[dict]] = {}
    for r in clustered:
        ment_by_cid.setdefault(r.get(COL_CID, "").strip(), []).append(r)

    rows_out: list[dict] = []
    excluded: list[tuple[str, str, str]] = []

    for r in core:
        if (r.get("deprecated") or "").strip() or (r.get("merged_into") or "").strip():
            continue
        name = r.get("name_yiddish") or r.get("name") or ""
        if not is_serial_list(name):
            continue

        db = r["db_id"]
        short = name[:46]

        if PLURAL.search(norm(name)):
            excluded.append((db, short, "plural head-noun — handled as a bundle"))
            continue
        if (r.get("org_type") or "").strip().lower() not in PERFORMING:
            excluded.append((db, short, f"org_type={r.get('org_type')!r} is not a performing type"))
            continue

        cids = [c.strip() for c in (r.get("linked_cluster_ids") or "").split("|") if c.strip()]
        mentions = []
        for c in cids:
            for m in ment_by_cid.get(c, []):
                mentions.append({
                    "cluster_id": c,
                    "host_entry": m.get(COL_HEAD, "").strip(),
                    "org_as_written": m.get(COL_ORG, "").strip(),
                    "settlement": m.get(COL_SETTLE, "").strip(),
                    "date_start": m.get(COL_DATE, "").strip(),
                    "sentence": m.get(COL_SENT, "").strip(),
                })
        if not mentions:
            excluded.append((db, short, f"{len(cids)} cluster(s), 0 mentions — nothing to judge (orphan row)"))
            continue

        # A singular head-noun ("דער טרופּע", "די טריאָ") is positive evidence of
        # ONE company; surface it rather than making the reviewer spot it.
        joined = norm(" ".join(m["sentence"] for m in mentions))
        singular_cue = bool(re.search(norm(r"דער טרופּע") + "|" + norm(r"די טרופּע")
                                      + "|" + norm(r"די טריאָ") + "|" + norm(r"אַ טרופּע"), joined))

        rows_out.append({
            "db_id": db,
            "name": r.get("name", ""),
            "name_yiddish": r.get("name_yiddish", ""),
            "org_type": r.get("org_type", ""),
            "n_names_in_title": str(norm(name).count(",") + (1 if AND_RE.search(norm(name)) else 0) + 1),
            "n_clusters": str(len(cids)),
            "n_mentions": str(len(mentions)),
            "singular_head_noun_cue": "yes" if singular_cue else "",
            "mentions_json": json.dumps(mentions, ensure_ascii=False),
        })

    rows_out.sort(key=lambda r: (-int(r["n_mentions"]), int(r["db_id"])))

    print(f"{len(rows_out)} rows to review  ({sum(1 for r in rows_out if r['singular_head_noun_cue'])} "
          f"carry a singular head-noun cue = likely ONE company)")
    for r in rows_out:
        cue = "  [singular cue]" if r["singular_head_noun_cue"] else ""
        print(f"  db{r['db_id']:<6} {r['n_mentions']:>2} mention(s)  "
              f"{(r['name_yiddish'] or r['name'])[:52]!r}{cue}")

    print(f"\nexcluded ({len(excluded)}):")
    for db, short, why in excluded:
        print(f"  db{db:<6} {short!r} — {why}")

    if not write:
        print("\n(dry-run; pass --write to emit the punchlist)")
        return

    fields = list(rows_out[0].keys())
    with OUT.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        w.writerows(rows_out)
    print(f"\nwrote {len(rows_out)} rows -> {OUT.name}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    main(ap.parse_args().write)
