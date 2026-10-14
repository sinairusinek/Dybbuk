#!/usr/bin/env python3
"""Second-pass retrieval: resolve the names the judge READ but could not pick.

When the judge abstains it may still name the person the sentences point to,
in `free_text_name` -- and crucially it supplies the GIVEN name, which the
span usually lacks (`לארגמאנ` -> `ליאַ לאָרגמאַן`). That is the search key the
surname index could never have.

Three outcomes, kept apart because they are different kinds of work:

  recovered   surname AND given name both match one hub -> an eponym proposal
              for the RA, same standing as any other.
  attested    no hub, but the name IS in the corpus: it appears as a
              `mention_name` in someone else's entry. A person the lexicon
              records without giving them an entry of their own. NOT an
              eponym answer and NOT a candidate -- an eponym edge points at a
              hub_id, and a mention has none until the person track mints one.
              This is the linkable subset of what used to be called `absent`.
  absent      the name is nowhere: no hub, and no mention either. The genuine
              corpus gap.
  ambiguous   several hubs match even with the given name -> needs a reader.

Why surname-only never counts as recovered: it generates confident nonsense.
`ליאַ לאָרגמאַן` matches `ברין, אַליאַ [גאָלדע זיידלין]` on the given name's
skeleton, and `זיגמונד טורקאָוו` matches `זיגמונד שוואַרץ`. Both are plainly
different people. An earlier draft of this idea used surname-only and would
have shipped those as links.

  python3 build_eponym_recall.py
"""
from __future__ import annotations

import argparse
import collections
import csv
import pathlib

from build_eponym_candidates import HEB, key, load_surname_index
from eponym_dossier import read_tsv

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
ORGS = BASE / "organizations"
VERDICTS = ORGS / "eponym_llm_verdicts.tsv"
CORE_DB = ORGS / "core_db.tsv"
MENTIONS = BASE / "people" / "mentions_all.tsv"
OUT = ORGS / "eponym_recall_candidates.tsv"

FIELDS = [
    "outcome", "db_id", "org_name", "span", "name_read_by_judge",
    "hub_id", "person_heading", "person_db_id", "n_hub_matches",
    "match_basis", "n_candidates_offered", "judge_reason", "judge_quote",
    "n_mentions", "mention_surfaces", "mention_hosts", "mention_sentence",
]


def load_mention_index() -> dict:
    """(surname, given) -> [mention rows].

    The hub table indexes ENTRY HEADINGS only, so `absent` has always meant
    "no entry heading matches" -- never "not in the corpus". mentions_all.tsv
    holds 38k people named inside other people's entries, which is where a
    theatre figure without an entry of their own actually lives.

    Keyed on surname+given TOGETHER, exactly as the hub lookup is, and for the
    same reason: a surname alone matches confident nonsense.
    """
    idx: dict[tuple, list] = {}
    for m in read_tsv(MENTIONS):
        toks = [t for t in HEB.findall(key(m.get("mention_name") or "")) if len(t) > 1]
        for i, a in enumerate(toks):
            for j, b in enumerate(toks):
                if i != j:
                    idx.setdefault((a, b), []).append(m)
    return idx


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--out", default=str(OUT))
    args = ap.parse_args()

    by_surname, by_full = load_surname_index()
    by_mention = load_mention_index()
    orgs = {r["db_id"]: r for r in read_tsv(CORE_DB)
            if (r.get("db_id") or "").strip()}
    rows = [r for r in read_tsv(VERDICTS)
            if (r.get("free_text_name") or "").strip()]

    out: list[dict] = []
    for r in rows:
        name = r["free_text_name"].strip()
        toks = [t for t in HEB.findall(key(name)) if len(t) > 1]

        hit, basis = None, ""
        # Strict: some token pair must match (surname, given) together.
        for i, a in enumerate(toks):
            for j, b in enumerate(toks):
                if i != j and (a, b) in by_full:
                    hit, basis = by_full[(a, b)], "surname+given"
                    break
            if hit:
                break
        if not hit:
            # Surname alone -- recorded, but NEVER as a recovery.
            for t in toks:
                if t in by_surname:
                    hit, basis = by_surname[t], "surname only"
                    break

        if basis == "surname+given" and len(hit) == 1:
            outcome = "recovered"
        elif basis == "surname+given":
            outcome = "ambiguous"
        else:
            outcome = "absent"

        # No usable hub -> ask whether the corpus knows the name at all, before
        # calling it a gap. Only for the unresolved: a recovered name already
        # has its hub, and re-describing it by its mentions adds nothing.
        mentions: list = []
        if outcome == "absent":
            for i, a in enumerate(toks):
                for j, b in enumerate(toks):
                    if i != j and (a, b) in by_mention:
                        mentions = by_mention[(a, b)]
                        break
                if mentions:
                    break
            if mentions:
                outcome = "attested"

        o = orgs.get(r["db_id"], {})
        h = hit[0] if hit else {}
        surfaces = sorted({(m.get("mention_name") or "").strip() for m in mentions})
        hosts = sorted({(m.get("host_heading") or "").strip() for m in mentions if m.get("host_heading")})
        sentence = next((s for s in ((m.get("relation_sentence") or "").strip()
                                     for m in mentions) if s), "")
        out.append({
            "outcome": outcome,
            "db_id": r["db_id"],
            "org_name": (o.get("name_yiddish") or "").strip()
                        or (o.get("name") or "").strip(),
            "span": r["span"],
            "name_read_by_judge": name,
            "hub_id": h.get("hub_id", "") if outcome == "recovered" else "",
            "person_heading": h.get("canonical_heading", "") if outcome == "recovered" else "",
            "person_db_id": (h.get("db_ids", "") or "").split(",")[0].strip()
                            if outcome == "recovered" else "",
            "n_hub_matches": len(hit) if hit else 0,
            "match_basis": basis,
            "n_candidates_offered": r.get("n_candidates", ""),
            "judge_reason": (r.get("reason") or "")[:300],
            "judge_quote": (r.get("quote") or "")[:300],
            "n_mentions": len(mentions),
            "mention_surfaces": " | ".join(surfaces[:6]),
            "mention_hosts": " | ".join(hosts[:6]),
            "mention_sentence": sentence[:300],
        })

    order = {"recovered": 0, "ambiguous": 1, "attested": 2, "absent": 3}
    out.sort(key=lambda d: (order[d["outcome"]], int(d["db_id"])))

    path = pathlib.Path(args.out)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        w.writeheader()
        w.writerows(out)

    counts = collections.Counter(d["outcome"] for d in out)
    print(f"names the judge read but could not pick: {len(rows)}")
    for k in ("recovered", "ambiguous", "attested", "absent"):
        print(f"  {k:10s} {counts.get(k, 0):4d}")
    print(f"\n  'attested' = no entry heading matches, but the name IS in the")
    print(f"  corpus as a mention in someone else's entry. Linkable once the")
    print(f"  person track mints an id -- not an eponym answer on its own.")
    print(f"  'absent'   = no hub and no mention: the genuine corpus gap.")
    # -o may point outside the tree (dry-running to a scratch dir), and
    # relative_to raises on that rather than falling back.
    try:
        shown = path.relative_to(BASE)
    except ValueError:
        shown = path
    print(f"\nwrote {shown}")


if __name__ == "__main__":
    main()
