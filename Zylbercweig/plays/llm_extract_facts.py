"""A4 — Gemini extraction of production facts from title-hit contexts.

Groups play_title_hits.tsv by host entry, builds merged context windows
around the hits, and asks Gemini to extract structured facts (one JSON
object per fact+participant) grounded in verbatim evidence quotes.

Surface forms only — entity identification happens later in
link_entities.py. The model never sees candidate DB entities.

Output: kg_extraction_drafts.tsv (advisory; resumable by window_id;
append-mode). Evidence quotes are machine-checked against the entry text;
failures get evidence_ok=no. A --pass2 mode re-runs already-drafted windows
with shuffled context order into kg_extraction_drafts_pass2.tsv for
agreement-based confidence triage.

Usage:
    GOOGLE_API_KEY=... python3.11 llm_extract_facts.py --limit 30 --stratified
    GOOGLE_API_KEY=... python3.11 llm_extract_facts.py
    GOOGLE_API_KEY=... python3.11 llm_extract_facts.py --pass2
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import plays_common as pc

DEFAULT_MODEL = "gemini-3-flash-preview"
PASS2_TSV = pc.HERE / "kg_extraction_drafts_pass2.tsv"
FLAGSHIP_PERSON_IDS = {
    "P-2-facs_90_tr_1744131428",        # Lateiner (extracted in-session, A3)
    "P-1-facs_312_TextRegion_1708860417410_829",  # Hurwitz
}
CONTEXT_PAD = 700
MAX_WINDOW_CHARS = 12000

SYSTEM_TEMPLATE = """You extract structured theater-history facts from entries of the Zylbercweig \
"Leksikon fun yidishn teater" (1931-1969), the standard biographical lexicon of the Yiddish \
theater. You read Yiddish natively (including pre-YIVO orthography, Hebrew-origin spellings, \
and OCR noise such as א/ע swaps and broken diacritics).

You receive numbered CONTEXT blocks from ONE person's biography entry. The entry's subject is \
given as HOST. Each context contains one or more candidate play titles (listed under \
CANDIDATE PLAYS with their registry id, claimed author, and ambiguity flags). The plays of \
interest are by the playwrights Joseph Lateiner (לאַטיינער) and Moses "Professor" Hurwitz \
(הורוויץ/הורוויטש). CAUTION: several candidate titles are also titles of famous plays by OTHER \
playwrights (flagged homonym_risk — e.g. „דער דיבוק" is usually Anski's play, „דער אוצר" \
usually Pinski's). Only connect a context mention to the candidate play when the context \
supports it (author named, known Lateiner/Hurwitz production, consistent era/venue); \
otherwise use fact_type "mention_only" with play_id_hint empty and note the likely true author.

Extract EVERY fact in the contexts about: a play's authorship, premiere, production, \
publication, translation/adaptation, or music; and every person's involvement in a \
production (acting + character, directing, composing, prompting, producing).

Reply with a strict JSON array. One object per (fact, participant) — a production naming 3 \
actors yields 3 objects sharing the evidence quote. Fields (all strings, "" when absent):

{{"play_title_surface": "title exactly as in the text",
  "play_id_hint": "registry id if the mention IS that play, else \\"\\"",
  "fact_type": "production|authorship|translation_adaptation|music|publication|premiere|mention_only",
  "person_surface": "name exactly as in text; \\"[HOST]\\" for the entry's subject",
  "person_role": "actor|director|composer|prompter|translator|adapter|producer|author|other",
  "character": "character played, if stated",
  "org_surface": "troupe/company as in text",
  "venue_surface": "theatre/venue as in text",
  "settlement_surface": "city/town as in text",
  "country": "country if stated or unambiguous",
  "date_start": "YYYY or YYYY-MM-DD", "date_end": "",
  "date_precision": "day|month|year|circa|none",
  "evidence_quote": "VERBATIM substring of the context (exact spelling/punctuation; it is \
machine-checked by exact string match)",
  "confidence": "high|medium|low",
  "notes": "oddities: disputed attribution, homonym suspicion, unclear referent"}}

Rules:
- NEVER invent dates, venues, or names not in the context.
- Copy surface spellings exactly; do not normalize.
- Every object MUST have a verbatim evidence_quote taken from the given contexts.
- The evidence_quote must be ONE contiguous span — never stitch distant fragments \
together with "..." or any ellipsis.
- If a context mentions a candidate title with no extractable fact, emit one mention_only object.
- Return [] if nothing is extractable.

Examples from this corpus:
=========== EXAMPLES ===========
{examples}
=========== END EXAMPLES ===========
"""


def load_hits(flagship: bool = False) -> list[dict]:
    """flagship=False -> corpus sweep minus the two playwright entries;
    flagship=True -> ONLY the two playwright entries (their own hits cover
    the repertoire sections densely, so hit windows ~ the whole entry)."""
    hits = pc.read_tsv(pc.TITLE_HITS_TSV)
    keep = []
    for h in hits:
        if not h["person_id"]:
            continue
        is_flag = h["person_id"] in FLAGSHIP_PERSON_IDS
        if is_flag != flagship:
            continue
        # prefix-tier citations are too weak to spend calls on without any
        # author signal in the entry (in the flagship entries the author is
        # the HOST, so keep everything there)
        if not flagship and h["tier"] == "P" and h["author_comention"] == "none":
            continue
        keep.append(h)
    return keep


def load_entry_texts() -> dict[str, dict]:
    return {e["person_id"]: e for e in pc.read_tsv(pc.ENTRY_TEXTS_TSV)}


def build_windows(hits: list[dict], entries: dict[str, dict],
                  plays_by_id: dict[str, dict]) -> list[dict]:
    """One window dict per (person_id, seq): merged context text + hit metadata."""
    by_person: dict[str, list[dict]] = defaultdict(list)
    for h in hits:
        by_person[h["person_id"]].append(h)

    windows = []
    for pid, phits in sorted(by_person.items()):
        entry = entries.get(pid)
        entry_hits = [h for h in phits if h["source"] == "entry_text"]
        covered_plays = {h["play_id"] for h in entry_hits}
        mention_hits = [h for h in phits
                        if h["source"] == "mention_sentence"
                        and h["play_id"] not in covered_plays]

        spans: list[tuple[int, int, dict]] = []
        if entry:
            raw = entry["entry_text"]
            for h in sorted(entry_hits, key=lambda x: int(x["char_start"])):
                s = max(0, int(h["char_start"]) - CONTEXT_PAD)
                e = min(len(raw), int(h["char_end"]) + CONTEXT_PAD)
                spans.append((s, e, h))
        merged: list[dict] = []  # {"start","end","hits"}
        for s, e, h in spans:
            if merged and s <= merged[-1]["end"]:
                merged[-1]["end"] = max(merged[-1]["end"], e)
                merged[-1]["hits"].append(h)
            else:
                merged.append({"start": s, "end": e, "hits": [h]})

        blocks: list[dict] = []  # {"text","hits"}
        for mrg in merged:
            blocks.append({"text": entry["entry_text"][mrg["start"]:mrg["end"]],
                           "hits": mrg["hits"]})
        # de-dup mention sentences (same sentence can host several hits)
        sent_seen: dict[str, dict] = {}
        for h in mention_hits:
            sent = h["context_before"] + h["matched_surface"] + h["context_after"]
            blk = sent_seen.get(sent)
            if blk:
                blk["hits"].append(h)
            else:
                sent_seen[sent] = {"text": sent, "hits": [h]}
        blocks.extend(sent_seen.values())
        if not blocks:
            continue

        # pack blocks into windows of <= MAX_WINDOW_CHARS
        cur, cur_len, seq = [], 0, 0
        heading = (entry or phits[0])["heading"] if entry else phits[0]["heading"]

        def flush():
            nonlocal cur, cur_len, seq
            if cur:
                seq += 1
                windows.append({
                    "window_id": f"{pid}:{seq}",
                    "person_id": pid,
                    "heading": heading,
                    "blocks": cur,
                })
                cur, cur_len = [], 0

        for blk in blocks:
            if cur and cur_len + len(blk["text"]) > MAX_WINDOW_CHARS:
                flush()
            cur.append(blk)
            cur_len += len(blk["text"])
        flush()

    for w in windows:
        w["hit_ids"] = "|".join(h["hit_id"] for b in w["blocks"] for h in b["hits"])
    return windows


def fmt_window(w: dict, plays_by_id: dict[str, dict], shuffle: bool = False) -> str:
    blocks = list(w["blocks"])
    if shuffle:
        random.shuffle(blocks)
    cand_lines = {}
    for b in blocks:
        for h in b["hits"]:
            p = plays_by_id.get(h["play_id"], {})
            flags = h["ambiguity_flag"] or "none"
            author = "Lateiner" if h["author_db_id"] == "683" else "Hurwitz"
            cand_lines[h["play_id"]] = (
                f"  - {h['play_id']}: {p.get('title_yiddish', '?')} "
                f"(claimed author: {author}; flags: {flags}; "
                f"matched: {h['matched_surface']!r})")
    parts = [f"HOST (entry subject): {w['heading']}",
             "CANDIDATE PLAYS:"] + sorted(cand_lines.values()) + [""]
    for i, b in enumerate(blocks, 1):
        parts.append(f"[CONTEXT {i}]\n{b['text']}\n")
    parts.append("Reply with the strict JSON array only.")
    return "\n".join(parts)


def build_few_shot(max_examples: int = 5) -> str:
    rows = pc.read_tsv(pc.FLAGSHIP_TSV)
    if not rows:  # bootstrap: verified gold rows share the schema
        rows = [r for r in pc.read_tsv(pc.GOLD_DIR / "gold_entries.tsv")
                if r.get("fact_type") in pc.FACT_TYPES]
    picked, seen_types = [], set()
    for r in rows:
        if r.get("evidence_ok") == "no" or not r.get("evidence_quote"):
            continue
        key = (r["fact_type"], bool(r.get("venue_surface")), bool(r.get("character")))
        if key in seen_types:
            continue
        seen_types.add(key)
        picked.append(r)
        if len(picked) >= max_examples:
            break
    out = []
    for r in picked:
        obj = {k: r.get(k, "") for k in (
            "play_title_surface", "play_id_hint", "fact_type", "person_surface",
            "person_role", "character", "org_surface", "venue_surface",
            "settlement_surface", "country", "date_start", "date_end",
            "date_precision", "evidence_quote", "confidence", "notes")}
        out.append(json.dumps(obj, ensure_ascii=False))
    return "\n".join(out) if out else "(none available)"


def parse_json_array(text: str) -> list[dict]:
    t = text.strip()
    if t.startswith("```"):
        lines = t.split("\n")
        t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
    i, j = t.find("["), t.rfind("]")
    if i >= 0 and j > i:
        try:
            data = json.loads(t[i:j + 1])
            if isinstance(data, list):
                return data
        except Exception:
            pass
    # salvage: a truncated array — pull out each complete top-level object
    out = []
    depth, start, in_str, esc = 0, -1, False, False
    for k, ch in enumerate(t):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                start = k
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                try:
                    out.append(json.loads(t[start:k + 1]))
                except Exception:
                    pass
                start = -1
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--stratified", action="store_true")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--pass2", action="store_true",
                    help="re-run already-drafted windows (shuffled context order) "
                         "into kg_extraction_drafts_pass2.tsv")
    ap.add_argument("--flagship", action="store_true",
                    help="process ONLY the two playwright entries into "
                         "kg_extraction_flagship.tsv")
    args = ap.parse_args()

    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY in env")
    os.environ["GOOGLE_API_KEY"] = api_key
    from google import genai
    from google.genai import types

    plays_by_id = {p["play_id"]: p for p in pc.load_plays_db()}
    entries = load_entry_texts()
    hits = load_hits(flagship=args.flagship)
    windows = build_windows(hits, entries, plays_by_id)
    print(f"hits in scope: {len(hits)}  windows: {len(windows)}  "
          f"entries: {len({w['person_id'] for w in windows})}")

    if args.flagship:
        out_path = pc.FLAGSHIP_TSV
    elif args.pass2:
        out_path = PASS2_TSV
    else:
        out_path = pc.DRAFTS_TSV
    if args.pass2:
        drafted = {r["window_id"] for r in pc.read_tsv(pc.DRAFTS_TSV)}
        done2 = {r["window_id"] for r in pc.read_tsv(PASS2_TSV)}
        work = [w for w in windows if w["window_id"] in drafted
                and w["window_id"] not in done2]
    else:
        done = {r["window_id"] for r in pc.read_tsv(out_path)}
        work = [w for w in windows if w["window_id"] not in done]

    if args.limit:
        if args.stratified:
            random.seed(20260725)
            random.shuffle(work)
        work = work[:args.limit]
    src_tag = "gemini_flagship" if args.flagship else "gemini_window"
    print(f"to do: {len(work)}  ->  {out_path.name}  model: {args.model}")
    if not work:
        print("nothing to do.")
        return

    few_shot = build_few_shot()
    system_prompt = SYSTEM_TEMPLATE.format(examples=few_shot)
    print(f"system prompt: ~{len(system_prompt):,} chars")

    client = genai.Client()
    mode = "a" if out_path.exists() else "w"
    n_facts = errors = bad_json = 0
    with out_path.open(mode, newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=pc.EXTRACTION_FIELDS, delimiter="\t",
                           extrasaction="ignore")
        if mode == "w":
            w.writeheader()
        for i, win in enumerate(work, 1):
            user_msg = fmt_window(win, plays_by_id, shuffle=args.pass2)
            try:
                resp = client.models.generate_content(
                    model=args.model,
                    contents=user_msg,
                    config=types.GenerateContentConfig(
                        system_instruction=system_prompt,
                        max_output_tokens=32768,
                        temperature=0.0,
                        thinking_config=types.ThinkingConfig(thinking_budget=0),
                    ),
                )
                text = (resp.text or "").strip()
            except Exception as e:
                print(f"  window {win['window_id']}: API error: {e}")
                errors += 1
                text = ""

            facts = parse_json_array(text)
            if text and not facts:
                bad_json += 1
            entry_text = entries.get(win["person_id"], {}).get("entry_text", "")
            window_text = "\n".join(b["text"] for b in win["blocks"])
            if not facts:
                # keep an empty marker row so resume skips this window
                w.writerow({
                    "fact_id": f"{win['window_id']}#0", "person_id": win["person_id"],
                    "xml_id": win["person_id"].split("-", 2)[-1], "source": src_tag,
                    "window_id": win["window_id"], "hit_ids": win["hit_ids"],
                    "fact_type": "none", "evidence_ok": "",
                    "model": args.model,
                    "drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "notes": "empty_or_unparseable" if text else "api_error",
                })
                f.flush()
                continue
            for n, obj in enumerate(facts, 1):
                if not isinstance(obj, dict):
                    continue
                quote = str(obj.get("evidence_quote") or "")
                ok = pc.check_evidence(quote, entry_text, window_text)
                row = {k: str(obj.get(k) or "") for k in pc.EXTRACTION_FIELDS}
                row.update({
                    "fact_id": f"{win['window_id']}#{n}",
                    "person_id": win["person_id"],
                    "xml_id": win["person_id"].split("-", 2)[-1],
                    "source": src_tag,
                    "window_id": win["window_id"],
                    "hit_ids": win["hit_ids"],
                    "evidence_ok": ok,
                    "model": args.model,
                    "drafted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                })
                w.writerow(row)
                n_facts += 1
            f.flush()
            if i % 10 == 0 or i == len(work):
                print(f"  {i}/{len(work)} windows  facts={n_facts}  "
                      f"api_errors={errors}  bad_json={bad_json}")

    print(f"\nwrote {out_path}  facts={n_facts}  api_errors={errors}  bad_json={bad_json}")


if __name__ == "__main__":
    main()
