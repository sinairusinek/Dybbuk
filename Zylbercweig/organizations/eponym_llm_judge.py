#!/usr/bin/env python3
"""Read an eponym dossier and propose which person an organisation is named for.

A PROPOSAL engine, not a decision engine. Verdicts land in
eponym_llm_verdicts.tsv tagged with the model that made them; only a human
confirmation enters eponym_decisions.tsv, and only that becomes an edge
(apply_eponym_decisions.py). Nothing here writes to the graph.

Two modes:
  --gold   re-judge the (db_id, span) pairs Sinai already decided, blind, and
           score against his answers. This GATES the open run: if precision on
           non-abstained verdicts is below the gate, the open set is not worth
           generating.
  --open   judge the still-open spans and write proposals for the RA workbook.

Grounding is enforced in code, not asked for politely: the model must return a
`quote`, and that quote must appear in the dossier it was shown (compared on
the point-stripped skeleton, so spelling-point noise doesn't fail a real
quote). A verdict whose quote cannot be found is downgraded to ABSTAIN with
grounding=failed. This is what separates a checkable verdict from a plausible
one -- and it is measured, see `grounding_failed` in the report.

Models: Gemini for bulk (the project's usage economy -- bulk LLM work goes to
Gemini, Claude for audit), Opus via --audit for a second opinion on
disagreements only.

  python3 eponym_llm_judge.py --gold --dry-run
  python3 eponym_llm_judge.py --gold --limit 40
  python3 eponym_llm_judge.py --open
  python3 eponym_llm_judge.py --audit 20
"""
from __future__ import annotations

import argparse
import collections
import csv
import datetime as _dt
import json
import os
import pathlib
import re
import sys
import time

import eponym_dossier as dossier
from eponym_dossier import key, read_tsv, sm

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[1]
ORGS = BASE / "organizations"
CANDIDATES = ORGS / "eponym_candidates.tsv"
DECISIONS = ORGS / "eponym_decisions.tsv"
GOLD_OUT = ORGS / "eponym_llm_gold_eval.tsv"
OPEN_OUT = ORGS / "eponym_llm_verdicts.tsv"

# Verified against client.models.list() on 2026-10-14. Five sibling scripts
# still name the retired gemini-3-pro-preview, which the API now 404s:
# llm_calibrate_gemini.py, llm_verify_shallow.py, llm_calibrate_gemini_retry.py,
# llm_redo_union_defaults.py and YiDraCor/code/vocalization/phonotactic_check.py.
# A 404 raised inside a retry loop looks exactly like slowness -- a run here
# sat 24 minutes at 6 seconds of CPU before anyone saw the error. Hence
# _preflight() below: one cheap call, raised not swallowed, before the loop.
GEMINI_MODEL = "gemini-3.1-pro-preview"
OPUS_MODEL = "claude-opus-5"

# Per-request deadline. Typical case: 7-50s depending on dossier size; a
# hung request is unbounded, hence this.
REQUEST_TIMEOUT_MS = 180_000

# Common nouns the namelist sweep mistakes for surnames -- same list the
# workbook builder uses, kept in sync by importing nothing: see
# build_eponym_ra_workbook.NOT_A_NAME. Imported lazily in _open_cases().

SYSTEM_PROMPT = """\
You identify whether a Yiddish theatre organisation is named after a person,
and which person, using ONLY the dossier you are given.

The dossier has the organisation's catalogue name, the sentences in the
Zylbercweig lexicon that attest it (each tagged with whose biography it comes
from, plus role, date and place), and a list of candidate people with their
own biographies.

Decide who the NAME IN QUESTION refers to.

Rules that matter here:
- You assert EPONYMY only -- that the organisation carries this person's name.
  You are NOT asserting they founded, owned, directed or performed in it. A
  sentence showing someone directed the troupe is strong evidence the troupe
  is named for them, but the claim you return is still only eponymy.
- An organisation named after several people (a trio, a partnership) has
  SEVERAL eponyms. You are asked about ONE name at a time. Never argue that
  the organisation should be split.
- Two adjacent surnames are usually two people, not one compound person.
- Initials do not identify a person. "ה. ב. עפשטיין" and "מ. ב. עפּשטיין" are
  different people; do not match on a surname when the initials disagree.
- Yiddish spelling varies. The same troupe appears as לערמאַנס, לערמאַנ'ס,
  לערמאַנקעס and לערמאַנקע; a person's name varies likewise. Treat these as
  the same name when the context supports it, and say so in your reason.
- Dates are decisive evidence. A person born in 1906 cannot be the eponym of a
  troupe attested in 1907. Check the candidate's life dates against the
  organisation's attested dates before choosing.
- A line marked "** MISMATCH" means the organisation's name carries an initial
  that disagrees with that candidate's given name (`י. קעסלערס טרופּע` is
  Yosef, not Dovid). Treat it as strong evidence AGAINST that candidate. The
  absence of a mismatch proves nothing.
- "entry size" lets you compare candidates: a person with a vast entry that
  discusses companies many times is a different kind of figure from one with a
  slight entry. This is context, not proof -- a famous actor is not the eponym
  of every troupe bearing his surname, and a manager with a short entry may
  well be.
- Lines marked "!" are sentences from that candidate's OWN biography that name
  this organisation. Their presence is good evidence; their absence is weak
  evidence, because most entries simply never name the company.
- If the dossier does not let you choose, ABSTAIN. An abstention is a correct
  and useful answer. Do not guess between candidates who are equally
  plausible, and do not pick the only candidate merely because it is the only
  one offered.

Reply with STRICT JSON, no prose and no code fence:

{"choice": "<hub_id> | NONE | ABSTAIN",
 "confidence": "high | medium | low",
 "quote": "<a verbatim span copied from the dossier that supports your choice>",
 "reason": "<one or two sentences>",
 "free_text_name": "<if no candidate fits but the dossier names the person, their name here; else empty>"}

"choice": a candidate's hub_id when one is the eponym; "NONE" when the name in
question is not a person at all (a place, a common noun, an institution);
"ABSTAIN" when it is a person but the dossier cannot tell you which.

"quote" must be copied character-for-character from the dossier. It is checked
mechanically. If you cannot supply a real quote, ABSTAIN.
"""


def parse_json_loose(text: str) -> dict:
    """Tolerate a code fence or stray prose around the JSON object."""
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.split("\n")
        t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
    try:
        return json.loads(t)
    except Exception:  # noqa: BLE001
        m = re.search(r"\{.*\}", t, re.S)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:  # noqa: BLE001
                return {}
        return {}


def check_grounding(quote: str, rendered: str) -> bool:
    """Is `quote` really in the dossier?

    Compared on the point-stripped, final-folded skeleton with whitespace
    collapsed: the model may normalise points or spacing when copying, and
    failing a genuine quote over a dagesh would make the check useless.
    """
    q = re.sub(r"\s+", " ", key(quote or "")).strip()
    if len(q) < 8:
        return False
    hay = re.sub(r"\s+", " ", key(rendered))
    return q in hay


# ── case construction ───────────────────────────────────────────────────────
def _candidates_by_span() -> dict[tuple, list[dict]]:
    out: dict[tuple, list[dict]] = collections.defaultdict(list)
    for c in read_tsv(CANDIDATES):
        out[(c["db_id"], c["span"])].append(c)
    return out


def gold_cases() -> list[dict]:
    """One case per (db_id, span) that Sinai decided, with the gold answer."""
    decisions = read_tsv(DECISIONS)
    by_span = _candidates_by_span()

    # Gold answer per span: the CONFIRMed hub, or NONE when every candidate for
    # that span was rejected.
    confirmed: dict[tuple, str] = {}
    rejected: dict[tuple, set] = collections.defaultdict(set)
    for d in decisions:
        k = (d["db_id"], d["span"])
        if (d.get("decision") or "").upper() == "CONFIRM":
            confirmed[k] = d["hub_id"]
        else:
            rejected[k].add(d["hub_id"])

    cases = []
    for k in sorted(set(confirmed) | set(rejected), key=lambda x: (int(x[0]), x[1])):
        group = by_span.get(k, [])
        hub_ids = [g["hub_id"] for g in group if g.get("hub_id")]
        if not hub_ids:
            continue
        gold = confirmed.get(k)
        if gold is None:
            # Every offered candidate rejected -> the right answer is NONE or
            # ABSTAIN; count either as correct in scoring.
            if not rejected.get(k):
                continue
            gold = "NONE"
        # Candidates that exist NOW but were not on the list when the decision
        # was made: everything the decision ledger never names for this span.
        # The ledger records one row per candidate shown, so a hub absent from
        # it was invisible at decision time.
        shown = {d["hub_id"] for d in decisions
                 if (d["db_id"], d["span"]) == k and d.get("hub_id")}
        cases.append({
            "db_id": k[0], "span": k[1],
            "hub_ids": list(dict.fromkeys(hub_ids)),
            "gold": gold,
            "hidden_hubs": tuple(h for h in hub_ids if h not in shown),
            "tier": group[0].get("tier", ""),
            "pattern": group[0].get("pattern", ""),
        })
    return cases


def open_cases() -> list[dict]:
    """Still-open spans -- the same selection the RA workbook uses."""
    from build_eponym_ra_workbook import NOT_A_NAME

    decisions = read_tsv(DECISIONS)
    confirmed = {(d["db_id"], d["span"]) for d in decisions
                 if (d.get("decision") or "").upper() == "CONFIRM"}
    decided_pairs = {f"{d['db_id']}|{d['hub_id']}" for d in decisions}
    by_span = _candidates_by_span()

    cases = []
    for k, group in sorted(by_span.items(), key=lambda kv: (int(kv[0][0]), kv[0][1])):
        if k in confirmed or k[1] in NOT_A_NAME:
            continue
        real = [g for g in group if g.get("hub_id")]
        if real and all(f"{g['db_id']}|{g['hub_id']}" in decided_pairs for g in real):
            continue
        cases.append({
            "db_id": k[0], "span": k[1],
            "hub_ids": list(dict.fromkeys(g["hub_id"] for g in real)),
            "gold": "",
            "tier": group[0].get("tier", ""),
            "pattern": group[0].get("pattern", ""),
        })
    return cases


# ── model calls ─────────────────────────────────────────────────────────────
def ask_gemini(client, rendered: str) -> tuple[dict, str]:
    from google.genai import types
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=rendered + "\n\nReply with strict JSON only.",
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            # ~260-360 of these are reasoning tokens before ~47 of JSON, so
            # 800 is headroom, not a tight budget. Measured 2026-10-14.
            max_output_tokens=800,
            temperature=0.0,
            # A request WILL occasionally hang with the socket open. Without a
            # timeout the run sleeps on it forever: one did for 34 minutes on
            # a single case, and because results were only written at the end,
            # killing it destroyed 90 completed verdicts. Never run this
            # without a client-side deadline.
            http_options=types.HttpOptions(timeout=REQUEST_TIMEOUT_MS),
        ),
    )
    text = (resp.text or "").strip()
    return parse_json_loose(text), text


def ask_opus(client, rendered: str) -> tuple[dict, str]:
    resp = client.messages.create(
        model=OPUS_MODEL,
        max_tokens=800,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user",
                   "content": rendered + "\n\nReply with strict JSON only."}],
    )
    text = resp.content[0].text.strip()
    return parse_json_loose(text), text


FIELDS = [
    "db_id", "span", "tier", "pattern", "n_candidates", "judge",
    "choice", "confidence", "grounding", "quote", "reason", "free_text_name",
    "proposal_hub_id", "proposal_heading", "proposal_person_db_id",
    "gold", "correct", "judged_at", "raw_error",
]


def hub_label(hub_id: str) -> tuple[str, str]:
    h = dossier._hubs().get(hub_id) or {}
    return ((h.get("canonical_heading") or "").strip(),
            (h.get("db_ids") or "").split(",")[0].strip())


def _preflight(client, judge: str) -> None:
    """One trivial call before the loop, with the error raised not swallowed.

    The retry loop in judge_cases() treats every exception as transient, which
    is right for a rate limit and badly wrong for a retired model id or a bad
    key: the run simply crawls, produces nothing, and reports no reason.
    """
    try:
        if judge == "gemini":
            from google.genai import types
            r = client.models.generate_content(
                model=GEMINI_MODEL, contents="Reply with the single word: ok",
                config=types.GenerateContentConfig(max_output_tokens=20,
                                                   temperature=0.0))
            reply = (r.text or "").strip()
        else:
            r = client.messages.create(
                model=OPUS_MODEL, max_tokens=20,
                messages=[{"role": "user",
                           "content": "Reply with the single word: ok"}])
            reply = r.content[0].text.strip()
    except Exception as e:  # noqa: BLE001
        model = GEMINI_MODEL if judge == "gemini" else OPUS_MODEL
        sys.exit(f"preflight failed for {model}: {type(e).__name__}: "
                 f"{str(e)[:300]}\n"
                 f"(nothing was judged; fix the model id or the key first)")
    print(f"  preflight ok: {GEMINI_MODEL if judge == 'gemini' else OPUS_MODEL} "
          f"-> {reply[:20]!r}")


def _ask_with_retries(client, judge: str, rendered: str, i: int,
                      db_id: str) -> tuple[dict, str, str]:
    """One case, up to 3 attempts. Returns (parsed, raw, last_error)."""
    t0 = time.time()
    err = ""
    for attempt in range(3):
        try:
            data, raw = (ask_gemini(client, rendered) if judge == "gemini"
                         else ask_opus(client, rendered))
            return data, raw, ""
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {str(e)[:160]}"
            # Say so. A silent retry loop made a dead model id and a hung
            # socket both look like ordinary slowness.
            print(f"    case {i} db{db_id} attempt {attempt + 1}/3 "
                  f"failed after {time.time() - t0:.0f}s: {err}", flush=True)
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
    return {}, "", err


def _append_row(path: pathlib.Path, row: dict, fields: list[str]) -> None:
    """Append one verdict immediately.

    Results used to be written only after the whole loop. A hung request 90
    cases into a 145-case run therefore cost all 90 when it was killed, and
    --resume had nothing to resume from. Each verdict is now durable the
    moment it exists.
    """
    new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t",
                           lineterminator="\n", extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow({**{k: "" for k in fields}, **row})


def judge_cases(cases: list[dict], judge: str, limit: int, dry_run: bool,
                sleep: float, out_path: pathlib.Path | None = None,
                fields: list[str] | None = None, workers: int = 1) -> list[dict]:
    client = None
    if not dry_run:
        if judge == "gemini":
            if not (os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")):
                sys.exit("GOOGLE_API_KEY (or GEMINI_API_KEY) not set")
            from google import genai
            client = genai.Client()
        else:
            if not os.environ.get("ANTHROPIC_API_KEY"):
                sys.exit("ANTHROPIC_API_KEY not set")
            import anthropic
            client = anthropic.Anthropic()
        _preflight(client, judge)

    sel = cases[:limit] if limit else cases
    out: list[dict] = []
    now = _dt.datetime.now().isoformat(timespec="seconds")
    t_start = time.time()

    if dry_run:
        for i, c in enumerate(sel, 1):
            rendered = dossier.render_case(
                dossier.build_case(c["db_id"], c["span"], c["hub_ids"]))
            if i == 1:
                print("=" * 72)
                print(f"SAMPLE PROMPT  (case 1 of {len(sel)})")
                print("=" * 72)
                print(f"[system: {len(SYSTEM_PROMPT):,} chars]")
                print(rendered[:3000])
                if len(rendered) > 3000:
                    print(f"… [{len(rendered) - 3000:,} more chars]")
                print("=" * 72)
            out.append({**c, "rendered_chars": len(rendered)})
        return out

    def judge_one(i_c: tuple[int, dict]) -> dict:
        i, c = i_c
        case = dossier.build_case(c["db_id"], c["span"], c["hub_ids"])
        rendered = dossier.render_case(case)
        data, raw, err = _ask_with_retries(client, judge, rendered, i, c["db_id"])

        choice = str(data.get("choice") or "").strip()
        quote = str(data.get("quote") or "").strip()
        conf = str(data.get("confidence") or "").strip().lower()
        grounded = check_grounding(quote, rendered)

        # A real hub choice without a findable quote is not usable.
        if choice and choice not in ("NONE", "ABSTAIN") and not grounded:
            choice, grounding = "ABSTAIN", "failed"
        elif not choice:
            choice, grounding = "ABSTAIN", "no_reply" if err or not raw else "unparsed"
        else:
            grounding = "ok" if grounded else "n/a"

        heading, pdb = hub_label(choice) if choice.startswith("HUB-") else ("", "")

        gold = c.get("gold") or ""
        correct = ""
        if gold:
            if gold == "NONE":
                if choice in ("NONE", "ABSTAIN"):
                    correct = "yes"
                elif choice in (c.get("hidden_hubs") or ()):
                    # Sinai answered NONE because the only candidate he was
                    # shown was the wrong namesake -- the comma bug hid 22% of
                    # hubs. If the judge names one of those hidden people, it
                    # has not contradicted his judgement; it found someone he
                    # could not have chosen. Scoring these as errors put the
                    # reported precision at 0.918 when every one of the eight
                    # was a recovery (Joseph Kessler, Aharon Rosenblum, Dina
                    # Kenig, Y. Y. Lerner, ...). Not scored; counted and
                    # reported separately as a find.
                    correct = "newly_visible"
                else:
                    correct = "no"
            elif choice == gold:
                correct = "yes"
            elif choice == "ABSTAIN":
                correct = "abstain"
            else:
                correct = "no"

        row = {
            "db_id": c["db_id"], "span": c["span"], "tier": c["tier"],
            "pattern": c["pattern"], "n_candidates": len(c["hub_ids"]),
            "judge": GEMINI_MODEL if judge == "gemini" else OPUS_MODEL,
            "choice": choice, "confidence": conf, "grounding": grounding,
            "quote": quote[:500], "reason": str(data.get("reason") or "")[:500],
            "free_text_name": str(data.get("free_text_name") or "")[:120],
            "proposal_hub_id": choice if choice.startswith("HUB-") else "",
            "proposal_heading": heading, "proposal_person_db_id": pdb,
            "gold": gold, "correct": correct, "judged_at": now,
            "raw_error": err,
        }
        if "n_distractors" in c:          # hardeval carries this through
            row["n_distractors"] = c["n_distractors"]
        return row

    done = 0
    if workers > 1:
        # Each verdict is independent, and the wall-clock cost is almost all
        # waiting on the API (a serial run shows ~0s CPU over an hour). The
        # 704-case open run is ~2 hours serially and about 15 minutes at 8.
        # Writes stay on this thread, under a lock, so the append-per-verdict
        # durability guarantee is unchanged.
        import concurrent.futures as _cf
        import threading
        # Warm the dossier caches on ONE thread first. They are
        # read-once-then-read-only, so a race could not corrupt them, but eight
        # threads each parsing the 34 MB cluster file simultaneously would
        # waste both time and memory.
        dossier.build_case(sel[0]["db_id"], sel[0]["span"], sel[0]["hub_ids"])
        lock = threading.Lock()
        with _cf.ThreadPoolExecutor(max_workers=workers) as pool:
            # submit() all up front, NOT pool.map over a generator: map pulls
            # from the iterable lazily and yields in order, so it kept only a
            # couple of requests in flight (2 threads, 0 open sockets, 35s in)
            # instead of saturating the pool.
            futs = [pool.submit(judge_one, ic) for ic in enumerate(sel, 1)]
            for fut in _cf.as_completed(futs):
                row = fut.result()
                with lock:
                    out.append(row)
                    if out_path is not None:
                        _append_row(out_path, row, fields or FIELDS)
                    done += 1
                    if done % 10 == 0 or done == len(sel):
                        print(f"  judged {done}/{len(sel)}  "
                              f"({time.time() - t_start:.0f}s elapsed)", flush=True)
    else:
        for i_c in enumerate(sel, 1):
            row = judge_one(i_c)
            out.append(row)
            if out_path is not None:
                _append_row(out_path, row, fields or FIELDS)
            done += 1
            if done % 10 == 0 or done == len(sel):
                print(f"  judged {done}/{len(sel)}  "
                      f"({time.time() - t_start:.0f}s elapsed)", flush=True)
            if sleep:
                time.sleep(sleep)
    return out


def score(rows: list[dict]) -> None:
    scored = [r for r in rows if r.get("gold")]
    if not scored:
        return
    # "newly_visible" = named a person the comma bug hid from the reviewer. Not
    # a contradiction of his judgement, so excluded from the precision
    # denominator and reported separately.
    newly = [r for r in scored if r["correct"] == "newly_visible"]
    dec = [r for r in scored
           if r["choice"] != "ABSTAIN" and r["correct"] != "newly_visible"]
    right = sum(1 for r in dec if r["correct"] == "yes")
    abst = sum(1 for r in scored if r["choice"] == "ABSTAIN")
    gf = sum(1 for r in rows if r["grounding"] == "failed")

    print()
    print("=" * 64)
    print(f"GOLD EVAL  ({len(scored)} cases with a known answer)")
    print("=" * 64)
    print(f"  answered (not abstained): {len(dec)}")
    print(f"  correct                 : {right}")
    print(f"  PRECISION on answered   : {right / len(dec):.3f}" if dec else "  n/a")
    print(f"  abstained               : {abst} ({abst * 100 // len(scored)}%)")
    print(f"  coverage (answered/all) : {len(dec) / len(scored):.3f}")
    print(f"  grounding failures      : {gf} ({gf * 100 // max(len(rows), 1)}%)")
    if newly:
        print(f"  FOUND a person the reviewer was never shown: {len(newly)}"
              "   (not errors -- recoverable links)")
        for r in newly:
            print(f"      db{r['db_id']:>5s} {r['span']:14s} -> "
                  f"{r['proposal_heading'][:34]}")
    print()
    print("  by tier:")
    by = collections.defaultdict(lambda: [0, 0, 0])
    for r in scored:
        b = by[r["tier"] or "?"]
        b[0] += 1
        if r["choice"] != "ABSTAIN" and r["correct"] != "newly_visible":
            b[1] += 1
            if r["correct"] == "yes":
                b[2] += 1
    for t, (n, a, c) in sorted(by.items()):
        p = f"{c / a:.2f}" if a else " n/a"
        print(f"    {t:12s} cases {n:4d}  answered {a:4d}  correct {c:4d}  precision {p}")

    wrong = [r for r in dec if r["correct"] == "no"]
    if wrong:
        print()
        print(f"  disagreements ({len(wrong)}) -- these are what to read:")
        for r in wrong[:12]:
            print(f"    db{r['db_id']:>5s} {r['span']:14s} chose {r['choice']:14s} "
                  f"gold {r['gold']:14s} [{r['confidence']}]")
            print(f"         {r['reason'][:150]}")


def main() -> None:
    ap = argparse.ArgumentParser()
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--gold", action="store_true")
    mode.add_argument("--open", dest="open_set", action="store_true")
    mode.add_argument("--audit", type=int, metavar="N",
                      help="re-judge N gold disagreements with Opus")
    ap.add_argument("--judge", choices=("gemini", "opus"), default="gemini")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.0)
    ap.add_argument("--workers", type=int, default=1,
                    help="concurrent requests (default 1). The 704-case open "
                         "run is ~2h serially, ~15min at 8.")
    ap.add_argument("--resume", action="store_true",
                    help="(default) skip (db_id, span) already in the output")
    ap.add_argument("--fresh", action="store_true",
                    help="delete the output file and judge every case again")
    args = ap.parse_args()

    if args.audit:
        prev = read_tsv(GOLD_OUT)
        wrong = [r for r in prev if r.get("correct") == "no"]
        if not wrong:
            sys.exit("no disagreements in the gold eval to audit")
        cases = [{"db_id": r["db_id"], "span": r["span"],
                  "hub_ids": [c["hub_id"] for c in read_tsv(CANDIDATES)
                              if c["db_id"] == r["db_id"] and c["span"] == r["span"]
                              and c.get("hub_id")],
                  "gold": r["gold"], "tier": r["tier"], "pattern": r["pattern"]}
                 for r in wrong[:args.audit]]
        out = ORGS / "eponym_llm_audit.tsv"
        if not args.dry_run and out.exists():
            out.unlink()
        rows = judge_cases(cases, "opus", 0, args.dry_run, args.sleep,
                           out_path=None if args.dry_run else out,
                           workers=args.workers)
    else:
        cases = gold_cases() if args.gold else open_cases()
        out = GOLD_OUT if args.gold else OPEN_OUT
        # Verdicts are appended as they are produced, so a kill costs at most
        # the case in flight. --resume then picks up from the file; --fresh
        # starts over.
        if args.fresh and out.exists() and not args.dry_run:
            out.unlink()
            print(f"--fresh: removed {out.name}")
        if out.exists() and not args.fresh:
            done = {(r["db_id"], r["span"]) for r in read_tsv(out)}
            before = len(cases)
            cases = [c for c in cases if (c["db_id"], c["span"]) not in done]
            print(f"resuming: {before - len(cases)} already in {out.name}, "
                  f"{len(cases)} to go")
        rows = judge_cases(cases, args.judge, args.limit, args.dry_run,
                           args.sleep, out_path=None if args.dry_run else out,
                           workers=args.workers)

    print(f"\ncases this run: {len(cases)}")
    if args.dry_run:
        tot = sum(r.get("rendered_chars", 0) for r in rows)
        print(f"dossier size: total {tot:,} chars, "
              f"mean {tot // max(len(rows), 1):,}, "
              f"max {max((r.get('rendered_chars', 0) for r in rows), default=0):,}")
        print(f"≈{tot // 3:,} input tokens for the run (plus system prompt each call)")
        print("\nDRY RUN -- no API calls, nothing written.")
        return

    allrows = read_tsv(out)        # everything on disk, this run and earlier
    print(f"{out.relative_to(BASE)}: {len(allrows)} rows total")
    score(allrows)


if __name__ == "__main__":
    main()
