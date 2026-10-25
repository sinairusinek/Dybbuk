"""Mint people / give hubs for blocked eponym answers — the pure half.

A blocked answer names a real person who has no hub, so no eponym edge can be
written (an edge carries `person_hub_id`). Resolving one means some of:

  * a new `people_db.tsv` row + its `new_mints_evidence.tsv` row (a MINT),
  * a db-only hub: a `person_hub.tsv` row + a `dbonly_hubs.tsv` ledger row,
    exactly as `people/add_dbonly_hubs.py` writes them,
  * CONFIRM rows in `eponym_decisions.tsv`, and the answer dropped from
    `eponym_blocked_answers.tsv`.

Everything here works on BYTES fetched from GitHub, never on the app's local
copy: on 2026-10-25 a stale local copy written whole reverted 2338 org_type
cells. New rows are appended to the latest file with its own line terminator,
and a new db_id is the GitHub high-water mark + 1 at the moment of saving.

The policy rules are the offline scripts' rules (README_new_mints.md):
`attested_in_leksikon=no` <=> `probably_not_zylbercweig=1`, and such a person
gets NO hub. Pure functions, no Streamlit, so it is testable.
"""
from __future__ import annotations

import csv
import io
import re
import unicodedata

csv.field_size_limit(10 ** 9)

EVIDENCE_TAG = "db_only_mention_attested"
ATTESTED = ("mentions_only", "no")


# ── TSV bytes ────────────────────────────────────────────────────────────────
def terminator(data: bytes) -> str:
    crlf = data.count(b"\r\n")
    return "\r\n" if crlf > (data.count(b"\n") - crlf) else "\n"


def parse(data: bytes) -> tuple[list[str], list[dict[str, str]]]:
    r = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""), delimiter="\t")
    return list(r.fieldnames or []), list(r)


def _serialise(fields: list[str], rows: list[dict], term: str, header: bool) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.DictWriter(buf, fieldnames=fields, delimiter="\t",
                       lineterminator=term, extrasaction="ignore", restval="")
    if header:
        w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8")


def append_rows(data: bytes, rows: list[dict]) -> bytes:
    """`data` with `rows` appended under its own header and line terminator.

    Existing bytes are kept verbatim -- no re-serialisation of rows this save
    did not touch, so the diff is exactly the new lines.
    """
    if not rows:
        return data
    term = terminator(data)
    fields, _ = parse(data)
    if data and not data.endswith(b"\n"):
        data += term.encode()
    return data + _serialise(fields, rows, term, header=False)


def drop_rows(data: bytes, keep) -> bytes:
    """Rewrite `data` keeping only rows where keep(row) is true."""
    fields, rows = parse(data)
    return _serialise(fields, [r for r in rows if keep(r)], terminator(data), header=True)


def next_db_id(*people_files: bytes) -> int:
    """GitHub high-water mark + 1, over every file that holds people ids."""
    top = 0
    for data in people_files:
        for r in parse(data)[1]:
            v = (r.get("db_id") or "").strip()
            if v.isdigit():
                top = max(top, int(v))
    return top + 1


# ── names ────────────────────────────────────────────────────────────────────
_FINALS = str.maketrans("םןךףץ", "מנכפצ")


def fold(s: str) -> str:
    nfd = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return s.translate(_FINALS).lower()


def skeleton(token: str) -> str:
    """Consonant skeleton: vowel letters dropped, so מאגידזאנ == מאַגיידזאָן.

    Used ONLY to choose which existing people to SHOW a reviewer beside a
    blocked answer. Never to resolve one: a skeleton match is how a sister
    gets her sibling's attestations (db864 Ida -> Regina).
    """
    t = fold(token)
    if re.search(r"[֐-׿]", t):
        # וו is the consonant v -- dropping it made קרילאוו (Krylov) קרל (Karl)
        t = t.replace("וו", "\0")
        return re.sub(r"[אויעה'\"״׳]", "", t).replace("\0", "וו")
    return re.sub(r"[aeiouyh']", "", t)


def _tokens(s: str) -> set[str]:
    out = set()
    for t in re.split(r"[^\w֐-׿']+", fold(s)):
        t = t.strip("'")
        if len(t) > 2:
            out.add(t)
    return out


def surname_keys(spans: list[str], answer: str) -> set[str]:
    """Skeletons of the spans (the troupe's own word for the eponym) plus the
    LAST token of the reviewer's answer. Given names are left out: matching
    on them offered every Kalmen and Shimen in the database."""
    keys = {skeleton(t) for s in spans for t in _tokens(s)}
    toks = [t for t in re.split(r"[^\w֐-׿']+", fold(answer)) if len(t) > 2]
    if toks:
        keys.add(skeleton(toks[-1].strip("'")))
    return {k for k in keys if len(k) >= 3}


def candidates(spans: list[str], answer: str, people: list[dict],
               hubs: list[dict], limit: int = 12) -> list[dict]:
    """Existing people whose name shares a surname skeleton with the answer."""
    want = surname_keys(spans, answer)
    hub_by_db = hubs_by_db_id(hubs)
    out = []
    for p in people:
        db_id = (p.get("db_id") or "").strip()
        if not db_id:
            continue
        hay = " ".join(p.get(k) or "" for k in
                       ("hebname", "english", "alternative_name", "name_variants"))
        got = {skeleton(t) for t in _tokens(hay)}
        if want & got:
            h = hub_by_db.get(db_id)
            out.append({
                "db_id": db_id,
                "hebname": (p.get("hebname") or "").strip(),
                "english": (p.get("english") or "").strip(),
                "born": (p.get("date_born") or "").strip(),
                "died": (p.get("date_died") or "").strip(),
                "hub_id": (h or {}).get("hub_id", ""),
                "heading": (h or {}).get("canonical_heading", ""),
                "not_zylbercweig": (p.get("probably_not_zylbercweig") or "").strip() == "1",
            })
    # people with a hub first: they resolve with no write beyond the decision
    out.sort(key=lambda c: (not c["hub_id"], c["hebname"]))
    return out[:limit]


def hubs_by_db_id(hubs: list[dict]) -> dict[str, dict]:
    out = {}
    for h in hubs:
        for d in re.split(r"[|,]", h.get("db_ids") or ""):
            if d.strip():
                out.setdefault(d.strip(), h)
    return out


# ── rows ─────────────────────────────────────────────────────────────────────
def person_row(db_id: str, yid: str, eng: str, gender: str, born: str, died: str,
               role: str, variants: str, attested: str, source: str) -> dict:
    return {
        "db_id": db_id, "hebname": yid, "english": eng, "gender": gender,
        "date_born": born, "date_died": died,
        "external_source": "Zylbercweig Leksikon (mentions, no entry)"
        if attested == "mentions_only" else "",
        "professional_role_perf": role, "name_variants": variants,
        # the README rule: only a person the Leksikon does not know is flagged
        "probably_not_zylbercweig": "1" if attested == "no" else "",
        "source": source,
        "raw": f"{eng}|{db_id}|{yid}" if eng else f"|{db_id}|{yid}",
    }


def evidence_row(db_id: str, yid: str, eng: str, attested: str, quote: str,
                 role: str, orgs: str, open_questions: str, reviewer: str,
                 date: str) -> dict:
    return {
        "db_id": db_id, "name_english": eng, "name_yiddish": yid,
        "attested_in_leksikon": attested, "leksikon_evidence": quote,
        "primary_attestation": "Zylbercweig Leksikon, mentioned in other "
        "people's entries" if attested == "mentions_only" else "",
        "source_type": "Leksikon mention" if attested == "mentions_only" else "",
        "role": role, "corroborating_people": f"eponym of core_db org(s) {orgs}",
        "open_questions": open_questions, "minted_by": reviewer, "minted_at": date,
    }


def hub_row(hub_fields: list[str], db_id: str, heading: str) -> dict:
    """A db-only hub, marked as add_dbonly_hubs.py marks it."""
    values = {
        "hub_id": f"HUB-D{db_id}", "canonical_heading": heading,
        "n_entries": "0", "n_db_rows": "1", "n_surfaces": "0",
        "entry_person_ids": "", "db_ids": db_id, "volumes": "",
        "evidence": EVIDENCE_TAG, "pending_drafts": "0",
        "multi_db": "0", "non_person": "0", "top_surfaces": "",
    }
    return {k: values.get(k, "") for k in hub_fields}


def ledger_row(db_id: str, heading: str, attested: str, reviewer: str, now: str) -> dict:
    return {
        "hub_id": f"HUB-D{db_id}", "db_id": db_id, "canonical_heading": heading,
        "reason": "eponym edge needs person_hub_id; person has no lexicon entry",
        "attested_in_leksikon": attested, "reviewer": reviewer, "added_at": now,
        "notes": "db-only hub, Zalmen eponym mint view; dropped by "
                 "build_person_hub.py -- re-run add_dbonly_hubs.py",
    }


def decision_row(blocked: dict, hub_id: str, heading: str, db_id: str,
                 reviewer: str, now: str, how: str) -> dict:
    return {
        "db_id": blocked["org_db_id"], "hub_id": hub_id, "span": blocked["span"],
        "decision": "CONFIRM", "tier": "", "pattern": "",
        "org_name_yiddish": blocked.get("org_name_yiddish", ""),
        "person_heading": heading, "person_db_ids": db_id,
        "reviewer": reviewer, "reviewed_at": now,
        "notes": f"Zalmen eponym mint view: {how}; answer was "
                 f"{blocked.get('reviewer_answer', '')!r}",
    }
