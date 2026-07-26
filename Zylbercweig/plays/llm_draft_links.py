"""B1c — Gemini adjudication drafter for the residual link-review queue.

Takes kg_link_review.tsv rows with no decision (after auto_triage_links.py),
shows Gemini the surface, its evidence contexts, and the candidate registry
entries, and drafts a decision:

  ACCEPT      the auto candidate link is the same entity
  REJECT      the candidate is a different entity (surface becomes a new node)
  NEW_PLAY    (play slot) real play absent from the registry
  NOT_ENTITY  surface is not an entity at all (character name, generic term)
  AMBIGUOUS   cannot tell -> stays queued for a human

High-confidence drafts are written into kg_link_review.tsv as
decision=GEMINI_<verdict>; AMBIGUOUS and low-confidence rows stay undecided.
Resumable via the drafted_at column.

Usage:
    GOOGLE_API_KEY=... python3.11 llm_draft_links.py [--limit N] [--execute]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

import plays_common as pc

DEFAULT_MODEL = "gemini-3-flash-preview"

SYSTEM = """You adjudicate entity links for a Yiddish theater knowledge graph built from the \
Zylbercweig lexicon. Native-grade Yiddish reading. For each item you get: a SLOT (person / \
org / venue / place / play), a SURFACE form from the lexicon text, up to 3 EVIDENCE contexts \
where it occurs, and the CANDIDATE entity the pipeline proposes (with registry names).

Reply strict single-line JSON:
{"verdict": "ACCEPT|REJECT|NEW_PLAY|NOT_ENTITY|AMBIGUOUS", "confidence": "high|medium|low", \
"rationale": "<=20 words"}

- ACCEPT: surface and candidate are the same entity (orthographic/vocalization variants,
  abbreviation, declension).
- REJECT: candidate is a different entity than the surface refers to.
- NEW_PLAY (play slot only, when no candidate fits): the surface is a real play title
  absent from the registry.
- NOT_ENTITY: the surface is not this kind of entity (a character name quoted like a
  title, a generic phrase, OCR garbage).
- AMBIGUOUS: genuinely undecidable from the evidence.
Be conservative: prefer AMBIGUOUS over a guessed ACCEPT."""


def gather_contexts() -> dict[tuple[str, str], list[str]]:
    linked = pc.read_tsv(pc.HERE / "kg_facts_linked.tsv")
    surf_cols = {"person": "person_surface", "org": "org_surface",
                 "venue": "venue_surface", "place": "settlement_surface",
                 "play": "play_title_surface"}
    ctx = defaultdict(list)
    for r in linked:
        for slot, col in surf_cols.items():
            s = r.get(col, "")
            if s and len(ctx[(slot, s)]) < 3 and r.get("evidence_quote"):
                ctx[(slot, s)].append(r["evidence_quote"][:300])
    return ctx


def candidate_desc(row: dict) -> str:
    link = row.get("auto_link", "")
    if not link:
        return "(no candidate — decide NEW_PLAY / NOT_ENTITY / AMBIGUOUS)"
    descs = []
    plays_by_id = {p["play_id"]: p for p in pc.load_plays_db()}
    people = {r["db_id"]: r for r in pc.read_tsv(pc.PEOPLE_DB_TSV) if r.get("db_id")}
    orgs = {r["db_id"]: r for r in pc.read_tsv(pc.ORGS_DIR / "core_db.tsv") if r.get("db_id")}
    for part in link.split("|"):
        ns, _, ref = part.partition(":")
        if ns == "play" and ref in plays_by_id:
            p = plays_by_id[ref]
            author = "Lateiner" if p["author_db_id"] == "683" else "Hurwitz"
            descs.append(f"{part}: {p['title_yiddish']} ({author}, status {p['attribution_status']})")
        elif ns == "person" and ref in people:
            pr = people[ref]
            descs.append(f"{part}: {pr.get('hebname','')} / {pr.get('english','')}")
        elif ns == "org" and ref in orgs:
            o = orgs[ref]
            descs.append(f"{part}: {o.get('name_yiddish','')} / {o.get('name','')} ({o.get('org_type','')})")
        else:
            descs.append(part)
    return "\n".join(descs)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--execute", action="store_true",
                    help="write GEMINI_* decisions into kg_link_review.tsv")
    args = ap.parse_args()

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY")
    from google import genai
    from google.genai import types

    rows = pc.read_tsv(pc.LINK_REVIEW_TSV)
    work = [r for r in rows if not r.get("decision")]
    if args.limit:
        work = work[:args.limit]
    print(f"queued surfaces: {len(work)}")
    if not work:
        return
    contexts = gather_contexts()
    client = genai.Client()

    from collections import Counter
    verdicts = Counter()
    applied = 0
    for i, r in enumerate(work, 1):
        ctxs = contexts.get((r["slot"], r["surface"]), [r.get("example_evidence", "")])
        msg = (f"SLOT: {r['slot']}\nSURFACE: {r['surface']}\n"
               f"AUTO METHOD: {r['auto_method']} (status {r['auto_status']})\n"
               f"CANDIDATE:\n{candidate_desc(r)}\n\nEVIDENCE:\n- " + "\n- ".join(ctxs)
               + "\n\nReply with strict JSON only.")
        try:
            resp = client.models.generate_content(
                model=args.model, contents=msg,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM, max_output_tokens=256,
                    temperature=0.0,
                    thinking_config=types.ThinkingConfig(thinking_budget=0)))
            text = (resp.text or "").strip()
        except Exception as e:
            print(f"  {r['slot']}/{r['surface'][:20]}: API error {e}")
            continue
        t = text
        if t.startswith("```"):
            t = "\n".join(t.split("\n")[1:-1])
        j0, j1 = t.find("{"), t.rfind("}")
        try:
            data = json.loads(t[j0:j1 + 1]) if j0 >= 0 else {}
        except Exception:
            data = {}
        v, conf = data.get("verdict", ""), data.get("confidence", "")
        verdicts[(v, conf)] += 1
        if v in ("ACCEPT", "REJECT", "NEW_PLAY", "NOT_ENTITY") and conf == "high":
            r["decision"] = f"GEMINI_{v}"
            r["decided_link"] = r["auto_link"] if v == "ACCEPT" else ""
            r["reviewer_notes"] = f"gemini: {data.get('rationale', '')}"
            applied += 1
        if i % 25 == 0 or i == len(work):
            print(f"  {i}/{len(work)}  applied={applied}")

    print("verdicts:", dict(verdicts))
    print(f"high-confidence applied: {applied}; still queued: {len(work) - applied}")
    if not args.execute:
        print("dry-run — decisions NOT saved (pass --execute)")
        return
    pc.write_tsv(pc.LINK_REVIEW_TSV, rows, list(rows[0].keys()))
    print(f"updated {pc.LINK_REVIEW_TSV}")


if __name__ == "__main__":
    main()
