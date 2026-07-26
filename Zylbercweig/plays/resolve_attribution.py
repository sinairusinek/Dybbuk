"""C3 — Deterministic resolution of play-attribution conflicts.

Zylbercweig prints each playwright's repertoire inside his own entry, so the
lexicon's OWN attribution of a title can be recovered by searching the title
in the two flagship entry texts. For every play flagged catalogue_conflict
or disputed, this reports where the title occurs and what that implies:

  lexicon_confirms_db     title only in the db-assigned author's entry
  lexicon_contradicts_db  title only in the OTHER author's entry
                          -> created_expressions row is likely wrong (swap
                             candidate; punchlist for Zalmen master data)
  both_entries            title in both entries -> plausibly two real
                          same-titled plays (keep disputed, PI call)
  unresolved              title in neither entry text

Writes eval/attribution_resolution.tsv and stamps matching eval_findings
rows adjudication=auto_lexicon_internal. Read-only w.r.t. people_db.

Usage:
    python3.11 resolve_attribution.py [--execute]
"""
from __future__ import annotations

import argparse
from collections import Counter

import plays_common as pc

FLAGSHIP = {"683": "P-2-facs_90_tr_1744131428",
            "684": "P-1-facs_312_TextRegion_1708860417410_829"}
OUT_TSV = pc.EVAL_DIR / "attribution_resolution.tsv"
FIELDS = ["play_id", "title_yiddish", "db_author", "catalogue_author",
          "in_lateiner_entry", "in_hurwitz_entry", "resolution",
          "recommendation", "matched_segment"]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args()

    entries = {e["person_id"]: e["entry_text"] for e in pc.read_tsv(pc.ENTRY_TEXTS_TSV)}
    norm_texts = {a: f" {pc.norm_yiddish(entries[pid])} "
                  for a, pid in FLAGSHIP.items()}

    plays = pc.load_plays_db()
    works = pc.read_tsv(pc.EVAL_DIR / "eval_reference_works.tsv")
    cat_author = {}
    for w in works:
        t = pc.norm_yiddish(w.get("title_yi", ""))
        a = (w.get("author") or "").split(".")[0]
        side = "683" if (a == "683" or "ateiner" in w.get("author", "")) else "684"
        cat_author.setdefault(t, set()).add(side)

    out, counts = [], Counter()
    for p in plays:
        if p["attribution_status"] not in ("catalogue_conflict", "disputed"):
            continue
        segs = [s for s in (p["title_segments_norm"] or "").split("|") if len(s) >= 5]
        found = {a: "" for a in ("683", "684")}
        for a, text in norm_texts.items():
            for seg in segs:
                if f" {seg} " in text:
                    found[a] = seg
                    break
        in_l, in_h = bool(found["683"]), bool(found["684"])
        db_a = p["author_db_id"]
        if in_l and in_h:
            res, rec = "both_entries", "keep disputed — plausibly two same-titled plays; PI call"
        elif not in_l and not in_h:
            res, rec = "unresolved", "title absent from both flagship entries; needs another source"
        else:
            lex_a = "683" if in_l else "684"
            if lex_a == db_a:
                res, rec = "lexicon_confirms_db", "keep db author; catalogue side likely wrong"
            else:
                res = "lexicon_contradicts_db"
                rec = (f"created_expressions likely wrong: move title from {db_a} "
                       f"to {lex_a} (punchlist for people_db)")
        counts[res] += 1
        cats = cat_author.get(pc.norm_yiddish(p["title_yiddish"]), set())
        out.append({
            "play_id": p["play_id"], "title_yiddish": p["title_yiddish"],
            "db_author": db_a, "catalogue_author": "|".join(sorted(cats)),
            "in_lateiner_entry": "yes" if in_l else "",
            "in_hurwitz_entry": "yes" if in_h else "",
            "resolution": res, "recommendation": rec,
            "matched_segment": found["683"] or found["684"],
        })

    print(f"conflicted plays examined: {len(out)}")
    print(dict(counts))
    if not args.execute:
        print("dry-run — pass --execute to write attribution_resolution.tsv")
        return
    pc.write_tsv(OUT_TSV, out, FIELDS)
    resolved = {r["play_id"] for r in out
                if r["resolution"] in ("lexicon_confirms_db", "lexicon_contradicts_db")}
    findings = pc.read_tsv(pc.EVAL_DIR / "eval_findings.tsv")
    n = 0
    for f in findings:
        if (f["aspect"] == "attribution" and f["bucket"] == "CONTRADICTED"
                and f["kg_ref"].removeprefix("play:") in resolved
                and not f.get("adjudication")):
            f["adjudication"] = "auto_lexicon_internal"
            n += 1
    pc.write_tsv(pc.EVAL_DIR / "eval_findings.tsv", findings, list(findings[0].keys()))
    print(f"wrote {OUT_TSV}; stamped {n} findings adjudication=auto_lexicon_internal")


if __name__ == "__main__":
    main()
