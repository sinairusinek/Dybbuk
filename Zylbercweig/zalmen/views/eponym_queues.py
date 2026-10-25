"""Eponym · review queues — READ-ONLY.

Three questions waiting on a reviewer, shown with the context needed to answer
them. **This view writes nothing.** Answers go in the Drive workbook
(`eponym_RA_workbook.xlsx`) as they do now; the retired `eponym_links` view
(deleted 2026-10-13, `d1feb21bc`) was the writing surface and is not coming
back here. Keeping this read-only avoids the two-surface problem that cost the
track 28 silently-dropped answers.

The three tabs, all derived from committed files and rebuilt by their own
scripts — never hand-edited here:

1. Spot-check (`eponym_spotcheck_queue.tsv`, build_eponym_spotcheck.py)
   CONFIRMed links worth a second look: `low` tier (a bare surname the
   generator could not disambiguate) or the span not appearing in the chosen
   person's heading. The second is the stronger signal: it caught db1047,
   where an answer had slipped onto a different surname.

   READ IT AS A QUESTION, NOT A CORRECTION. A surname mismatch means *one of
   the two names is wrong*, and the evidence decides which: on db287 the ORG
   name was the error (Bergman upheld); on db1047 the MATCH was. Same flag,
   opposite outcomes.

2. Blocked answers (`eponym_blocked_answers.tsv`, written by the ingest)
   Answers naming a real person who has no hub, so no edge can be written yet.
   An eponym edge carries `person_hub_id`; `person_hub.tsv` is built only from
   lexicon ENTRIES, so a mention-attested person needs a people_db row and then
   a hub. Resolving these by name similarity is unsafe — "אידא קאמינסקי" would
   match Regina Kaminska (d.1913), Ida's sister.

3. Org type · OTHER (read live from `core_db.tsv`)
   Orgs parked at the literal type `OTHER - elaborate!`, which is a placeholder
   asking for a real type. Not strictly eponym work, but it is reviewer input
   of the same kind and has had no owner.
"""
from __future__ import annotations

import csv
import pathlib

import streamlit as st

BASE = pathlib.Path(__file__).resolve().parents[2]
ORGS = BASE / "organizations"
SPOTCHECK = ORGS / "eponym_spotcheck_queue.tsv"
BLOCKED = ORGS / "eponym_blocked_answers.tsv"
CORE_DB = ORGS / "core_db.tsv"
DECISIONS = ORGS / "eponym_decisions.tsv"

OTHER_TYPE = "OTHER - elaborate!"
WORKBOOK = (
    "https://docs.google.com/spreadsheets/d/"
    "1kPuKA8E4EnYnbf_fbFSVHyckEHI0TuDd/edit"
)


@st.cache_data(show_spinner=False)
def _read(path_str: str, mtime: float) -> list[dict]:
    """Read a TSV. `mtime` is in the cache key so an edited file re-reads."""
    csv.field_size_limit(10 ** 9)
    p = pathlib.Path(path_str)
    if not p.exists():
        return []
    with p.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _load(p: pathlib.Path) -> list[dict]:
    return _read(str(p), p.stat().st_mtime if p.exists() else 0.0)


def _rtl(s: str) -> str:
    """Wrap Hebrew/Yiddish so it renders RTL beside Latin text."""
    return f'<span dir="rtl" style="font-size:1.05em">{s}</span>' if s else ""


def _missing(p: pathlib.Path, how: str) -> None:
    st.warning(f"`{p.name}` is not in the repo yet. Rebuild it with `{how}`.")


# ── tab 1: spot-check ────────────────────────────────────────────────────────
def _spotcheck() -> None:
    rows = _load(SPOTCHECK)
    if not rows:
        _missing(SPOTCHECK, "python3.11 build_eponym_spotcheck.py --write")
        return

    strong = [r for r in rows if "span-not-in-heading" in r.get("flag", "")]
    st.caption(
        f"{len(rows)} CONFIRMed links flagged for a second look · "
        f"**{len(strong)}** where the span does not appear in the person's "
        f"heading (the stronger signal)"
    )
    st.info(
        "A surname mismatch means **one of the two names is wrong** — not that "
        "the match is wrong. On db287 the org name was the error and Bergman "
        "stood; on db1047 the match itself had slipped. Quickest check: does "
        "the *given* name match exactly, and is this the only person in the "
        "corpus with that given name?",
        icon="💡",
    )

    c1, c2 = st.columns([2, 1])
    with c1:
        only_strong = st.checkbox(
            "Show only span-not-in-heading", value=True, key="ep_strong",
            help="Hides the low-tier-only rows, which are ambiguity rather "
                 "than a possible slip.",
        )
    with c2:
        reviewers = sorted({r.get("reviewer", "") for r in rows if r.get("reviewer")})
        who = st.selectbox("Reviewer", ["(all)"] + reviewers, key="ep_rev")

    shown = [r for r in rows
             if (not only_strong or "span-not-in-heading" in r.get("flag", ""))
             and (who == "(all)" or r.get("reviewer") == who)]
    st.caption(f"{len(shown)} of {len(rows)} rows")

    for r in shown:
        org = r.get("org_name", "")
        st.markdown(
            f"**db{r.get('db_id','')}** &nbsp; {_rtl(org)}", unsafe_allow_html=True
        )
        st.markdown(
            f"&nbsp;&nbsp;span {_rtl(r.get('span',''))} &nbsp;→&nbsp; "
            f"{_rtl(r.get('person_heading',''))} "
            f"<code>{r.get('hub_id','')}</code>",
            unsafe_allow_html=True,
        )
        bits = [r.get("flag", ""), f"tier {r.get('tier','')}",
                r.get("pattern", ""), r.get("reviewer", "")]
        st.caption(" · ".join(b for b in bits if b))
        st.divider()


# ── tab 2: blocked answers ───────────────────────────────────────────────────
def _blocked() -> None:
    rows = _load(BLOCKED)
    if not rows:
        _missing(BLOCKED, "python3.11 ingest_eponym_ra_workbook.py <workbook>")
        return

    # Several answers name the SAME person across different orgs, so the row
    # count overstates the number of decisions needed. Group by the answer.
    by_person: dict[str, list[dict]] = {}
    for r in rows:
        by_person.setdefault(r.get("reviewer_answer", ""), []).append(r)

    st.caption(
        f"{len(rows)} answers across **{len(by_person)} distinct people** — "
        "each names someone real who has no hub, so no edge can be written yet"
    )
    st.info(
        "An eponym edge carries `person_hub_id`, and `person_hub.tsv` is built "
        "only from lexicon **entries** — so a person the Leksikon merely "
        "*mentions* has no hub. These need a people_db row (a mint) and then "
        "a hub. They must **not** be matched by name similarity alone: "
        "*אידא קאמינסקי* would land on Regina Kaminska, Ida's sister, who "
        "died in 1913. **Resolve them in Eponym · mint people.**",
        icon="🚧",
    )

    for answer, group in sorted(by_person.items(), key=lambda kv: -len(kv[1])):
        pdb = next((g.get("has_people_db_row", "") for g in group
                    if g.get("has_people_db_row", "").strip()), "")
        head = f"**{answer}**" if answer else "**(blank answer)**"
        if len(group) > 1:
            head += f" &nbsp;· named in {len(group)} orgs"
        st.markdown(head, unsafe_allow_html=True)
        for g in group:
            st.markdown(
                f"&nbsp;&nbsp;db{g.get('org_db_id','')} &nbsp;"
                f"{_rtl(g.get('org_name_yiddish',''))} &nbsp;· span "
                f"{_rtl(g.get('span',''))}",
                unsafe_allow_html=True,
            )
        st.caption(
            f"people_db {pdb} — needs a hub only" if pdb
            else "no people_db row — needs a mint, then a hub"
        )
        st.divider()


# ── tab 3: org type · OTHER ──────────────────────────────────────────────────
def _other_types() -> None:
    rows = [r for r in _load(CORE_DB)
            if (r.get("org_type") or "").strip() == OTHER_TYPE
            and not (r.get("deprecated") or "").strip()
            and not (r.get("merged_into") or "").strip()]
    if not rows:
        st.success(f"No live orgs are parked at `{OTHER_TYPE}`.")
        return

    st.caption(
        f"{len(rows)} live orgs typed `{OTHER_TYPE}` — the value is a "
        "placeholder asking for a real type, not a type"
    )
    st.info(
        "Not eponym work, but reviewer input of the same kind and so far "
        "unowned. Mostly institutes, youth clubs and student unions. Retype "
        "them in **Organization Cards**, which is the writing surface for "
        "`org_type`.",
        icon="🏷",
    )
    for r in rows:
        st.markdown(
            f"**db{r.get('db_id','')}** &nbsp; "
            f"{_rtl((r.get('name_yiddish') or '').strip())} &nbsp; "
            f"{(r.get('name') or '').strip()}",
            unsafe_allow_html=True,
        )
        note = (r.get("notes") or "").strip()
        if note:
            st.caption(note[:200])
        st.divider()


def render() -> None:
    st.header("Eponym · review queues")
    st.caption(
        "Read-only. Answers go in the Drive workbook — this view shows what is "
        "waiting and the context to judge it."
    )
    st.markdown(f"[Open the RA workbook]({WORKBOOK})")

    dec = _load(DECISIONS)
    if dec:
        n_conf = sum(1 for d in dec if d.get("decision") == "CONFIRM")
        st.caption(
            f"Ledger: {len(dec)} decisions · {n_conf} CONFIRM · "
            f"{len(dec) - n_conf} REJECT"
        )

    t1, t2, t3 = st.tabs([
        "① Spot-check", "② Blocked answers", f"③ Org type · {OTHER_TYPE}",
    ])
    with t1:
        _spotcheck()
    with t2:
        _blocked()
    with t3:
        _other_types()
