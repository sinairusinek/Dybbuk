"""Cross-links between the Plays KG and the YiDraCor entity tree.

Two pages describe the same Lateiner/Hurwitz works from different sources:

  * the Plays KG (plays_kg_graph.html), built from Lexicon facts over
    plays_db.tsv, which carries the ascription verdicts and the per-fact
    attribution guard;
  * the entity tree (lateiner_hurwitz_entities.html), built from the
    catalogue workbook + YiDraCor editions (YiDraCor/data/entity_graph.json).

This module joins them and attaches the open questions, so both pages show
the same verdicts and the same questions:

  work_to_play()   catalogue work id -> plays_db play_id
                   (expression id recorded in plays_db notes, else the exact
                   normalised title, disambiguated by playwright)
  questions()      every question in REVIEW_TASKS.md, its status read from the
                   file itself (an empty "> Answer:" = open), joined to the
                   plays in open_questions.tsv (`@unvetted` = every play with
                   attribution_status=ascription_unvetted); questions marked
                   applied there may have left REVIEW_TASKS.md for
                   REVIEW_DECISIONS.md
  play_summary()   per play: verdict, author, Lexicon facts in the graph and
                   excluded from it, the questions touching it
  disagreements()  where the catalogue and plays_db disagree on the author or
                   on whether the ascription was rejected

REVIEW_TASKS.md stays the one place questions are asked and answered;
open_questions.tsv only says which plays each question concerns.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict

import plays_common as pc

csv.field_size_limit(sys.maxsize)

REVIEW_TASKS_MD = pc.HERE / "REVIEW_TASKS.md"
OPEN_QUESTIONS_TSV = pc.HERE / "open_questions.tsv"
ENTITY_GRAPH_JSON = pc.HERE.parent.parent / "YiDraCor" / "data" / "entity_graph.json"
EXCLUDED_TSV = pc.KG_DIR / "excluded_play_facts.tsv"
# where Ruthie reads and answers the questions (REVIEW_TASKS.md mirrors it)
REVIEW_TASKS_URL = ("https://docs.google.com/document/d/"
                    "1NYPJTWdP95GCjXBeSHQBl5bTdqRcvBab8FdmedhJuic/edit")

PLAYWRIGHT_DB_ID = {"Lateiner": "683", "Hurwitz": "684"}
AUTHOR_NAME = {"683": "Lateiner", "684": "Hurwitz"}
REJECTED_IN_CATALOGUE = {"false ascription", "error", "uncertain/false?"}

_QHEAD = re.compile(r"^### ([A-Z]+\d+)\.\s*(.*)$")
_ANSWER = re.compile(r"^>\s*Answers?(?:\s*/\s*notes)?\s*:\s*(.*)$", re.M)


def _parse_review_tasks() -> dict[str, dict]:
    """qid -> {title, audience, answered, answer} from REVIEW_TASKS.md.

    Questions are headed `### <qid>. <title>`; every question there is for
    Ruthie. Answered-and-applied questions move to REVIEW_DECISIONS.md."""
    text = REVIEW_TASKS_MD.read_text(encoding="utf-8")
    out: dict[str, dict] = {}
    for b in re.split(r"^(?=#{2,3} )", text, flags=re.M):
        m = _QHEAD.match(b.split("\n", 1)[0])
        if not m:
            continue
        am = _ANSWER.search(b)
        answer = ""
        if am:
            tail = (am.group(1) + "\n" + b[am.end():]).strip()
            answer = re.sub(r"^-{3,}\s*$", "", tail, flags=re.M).strip()
        out[m.group(1)] = {"qid": m.group(1), "title": m.group(2).strip(),
                           "audience": "Ruthie (PI)",
                           "answered": bool(answer), "answer": answer}
    return out


def questions(plays: dict[str, dict] | None = None) -> list[dict]:
    """All REVIEW_TASKS questions with status and the plays they touch."""
    plays = plays or {p["play_id"]: p for p in pc.load_plays_db()}
    parsed = _parse_review_tasks()
    mapping = {r["qid"]: r for r in pc.read_tsv(OPEN_QUESTIONS_TSV)}
    missing = sorted(q for q in set(mapping) - set(parsed)
                     if not (mapping[q].get("applied") or "").strip())
    if missing:
        raise SystemExit(f"open_questions.tsv names open questions not in "
                         f"REVIEW_TASKS.md: {missing}")
    unmapped = sorted(set(parsed) - set(mapping))
    if unmapped:
        raise SystemExit(f"REVIEW_TASKS.md questions missing from "
                         f"open_questions.tsv: {unmapped}")
    unvetted = [pid for pid, p in plays.items()
                if p["attribution_status"] == "ascription_unvetted"]
    out = []
    for qid in list(parsed) + [q for q in mapping if q not in parsed]:
        m = mapping.get(qid, {})
        q = parsed.get(qid) or {"qid": qid, "title": m.get("summary", ""),
                                "audience": "Ruthie (PI)", "answered": True,
                                "answer": ""}
        pids: list[str] = []
        for tok in (m.get("play_ids") or "").split("|"):
            tok = tok.strip()
            if tok == "@unvetted":
                pids += unvetted
            elif tok:
                if tok not in plays:
                    raise SystemExit(f"open_questions.tsv {qid}: unknown {tok}")
                pids.append(tok)
        status = ("applied" if (m.get("applied") or "").strip()
                  else "answered" if q["answered"] else "open")
        out.append({**q, "status": status, "play_ids": pids,
                    "edition_tkb": (m.get("edition_tkb") or "").strip(),
                    "summary": (m.get("summary") or "").strip() or q["title"],
                    "url": REVIEW_TASKS_URL})
    return out


def work_to_play(works: list[dict], plays: dict[str, dict]) -> dict[str, str]:
    """catalogue work node id -> play_id ('' when plays_db has no such play)."""
    by_eid: dict[str, str] = {}
    by_title: dict[str, set[str]] = defaultdict(set)
    for p in plays.values():
        m = re.search(r"\beid (\d+)", p.get("notes", ""))
        if m:
            by_eid[m.group(1)] = p["play_id"]
        by_title[pc.norm_yiddish(p["title_yiddish"])].add(p["play_id"])
    out = {}
    for w in works:
        pid = by_eid.get(str(w.get("expression_id") or ""))
        if not pid:
            cands = by_title.get(pc.norm_yiddish(w.get("yiddish_title") or ""), set())
            if len(cands) > 1:
                aid = PLAYWRIGHT_DB_ID.get(w.get("playwright", ""))
                cands = {c for c in cands if plays[c]["author_db_id"] == aid} or cands
            pid = next(iter(cands)) if len(cands) == 1 else ""
        out[w["id"]] = pid
    return out


def _fact_counts() -> tuple[Counter, Counter]:
    """(facts on graph edges, facts excluded) per play_id."""
    kept: dict[str, set] = defaultdict(set)
    for e in pc.read_tsv(pc.KG_DIR / "edges.tsv"):
        for v in (e["source_id"], e["target_id"]):
            if v.startswith("play:PL-"):
                for f in (e.get("provenance_fact_ids") or "").split("|"):
                    if f:
                        kept[v[5:]].add(f)
    excl: dict[str, set] = defaultdict(set)
    for r in pc.read_tsv(EXCLUDED_TSV):
        link = r.get("play_link", "")
        if link.startswith("play:PL-") and "|" not in link:
            excl[link[5:]].add(r["fact_id"])
    return (Counter({k: len(v) for k, v in kept.items()}),
            Counter({k: len(v) for k, v in excl.items()}))


def play_summary(plays: dict[str, dict] | None = None,
                 qs: list[dict] | None = None) -> dict[str, dict]:
    plays = plays or {p["play_id"]: p for p in pc.load_plays_db()}
    qs = qs if qs is not None else questions(plays)
    kept, excl = _fact_counts()
    by_play: dict[str, list[str]] = defaultdict(list)
    for q in qs:
        if q["status"] != "applied":
            for pid in q["play_ids"]:
                by_play[pid].append(q["qid"])
    return {pid: {"play_id": pid,
                  "status": p["attribution_status"],
                  "author": AUTHOR_NAME.get(p["author_db_id"], p["author_heading"]),
                  "n_facts": kept.get(pid, 0),
                  "n_excluded": excl.get(pid, 0),
                  "questions": by_play.get(pid, [])}
            for pid, p in plays.items()}


def disagreements(work: dict, play: dict | None) -> list[str]:
    """Plain-language catalogue-vs-Plays-KG disagreements for one work."""
    if play is None:
        return ["not in the Plays KG play list"]
    out = []
    cat_author = work.get("playwright", "")
    if PLAYWRIGHT_DB_ID.get(cat_author) and \
            PLAYWRIGHT_DB_ID[cat_author] != play["author_db_id"]:
        out.append(f"catalogue sheet: {cat_author}; Plays KG: "
                   f"{AUTHOR_NAME.get(play['author_db_id'], '?')}")
    cat_rej = (work.get("attribution") or "") in REJECTED_IN_CATALOGUE
    kg_rej = play["attribution_status"] == "ascription_rejected"
    if cat_rej and not kg_rej:
        out.append(f"catalogue: {work.get('attribution')}; Plays KG still "
                   f"treats the ascription as valid")
    elif kg_rej and not cat_rej:
        out.append(f"Plays KG: ascription rejected; catalogue: "
                   f"{work.get('attribution') or 'blank'}")
    return out


def load_entity_works() -> list[dict]:
    g = json.loads(ENTITY_GRAPH_JSON.read_text(encoding="utf-8"))
    return [n for n in g["nodes"] if n.get("kind") == "work"]


def main() -> None:
    plays = {p["play_id"]: p for p in pc.load_plays_db()}
    qs = questions(plays)
    print(Counter(q["status"] for q in qs))
    for q in qs:
        print(f"  {q['qid']:4} {q['status']:8} {len(q['play_ids']):3} plays  {q['summary'][:70]}")
    works = load_entity_works()
    w2p = work_to_play(works, plays)
    print(f"works mapped: {sum(1 for v in w2p.values() if v)}/{len(works)}")
    for w in works:
        d = disagreements(w, plays.get(w2p[w["id"]]))
        if d:
            print(f"  {w2p[w['id']] or '—':8} {w.get('yiddish_title')}: {'; '.join(d)}")


if __name__ == "__main__":
    main()
