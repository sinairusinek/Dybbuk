"""Eponym links — confirm that an org named after a person links that person.

The gap this closes: `graph/person_org_edges.tsv` holds ~16.9k edges of two
sources (`mention`, `plays_kg`), and every one means "this org was named in
this person's biography". That is a DIFFERENT relation from "this org is named
after this person". db641 `לערמאַנס טרופּע` has 14 performer edges and not
Lerman; db1062 `ראָמעס, שוואַרץ און שאָר` links none of its three namesakes.

Input : organizations/eponym_candidates.tsv   (build_eponym_candidates.py)
Output: organizations/eponym_decisions.tsv    (via troupe_store, data branch)
        → apply_eponym_decisions.py writes edge_source=eponym edges.

Tiers drive the layout, because the buckets differ in kind, not just in degree:

  ⚡ high (68)   surname + an ADJACENT given name, exactly one hub. Checkable at
                a glance — `טרופּע פֿון אַברהם פישזאָן` → `פישזאָן, אַברהם`.
                Batch table, tick-and-confirm.
  ◐ medium      unique surname, no given name to corroborate. Batch table too,
                but unticked by default — a unique match is not a sure one.
  ? low         the surname is ambiguous (פֿרידמאַן = 15 hubs) or sits in a
                multi-name list. One card per org, every candidate person shown
                as a radio: picking a person IS the decision. Never auto-ticked.
  ✗ unresolved  no hub matched the span. Read-only; these need a people-side
                fix (a missing hub, or a spelling variant), not a tick here.

An eponym edge asserts eponymy ONLY — not founding, not ownership, not a role.
`people_db.tsv.professional_role_org` stays empty: a role is a separate claim
and nothing on this screen establishes one.

A multi-name org yields SEVERAL confirmed people and is never a reason to split
the org — the 16 joint-name rows were all settled as ONE company each on
2026-10-05. Decisions are keyed (db_id, hub_id) so each person is independent.
"""
from __future__ import annotations

import csv
import datetime as _dt
import html
import pathlib

import streamlit as st

import troupe_store

csv.field_size_limit(10 ** 9)

BASE = pathlib.Path(__file__).resolve().parents[2]
CANDIDATES = BASE / "organizations" / "eponym_candidates.tsv"

PAGE_SIZE = 40
TIER_ORDER = ["high", "medium", "low", "unresolved"]
TIER_LABEL = {
    "high": "⚡ High · surname + given name",
    "medium": "◐ Medium · unique surname only",
    "low": "? Low · ambiguous or multi-name",
    "unresolved": "✗ Unresolved · no person matched",
}

_RTL = ("<div dir='rtl' style='font-size:{size}rem;font-weight:{w};"
        "line-height:1.5'>{text}</div>")


def _rtl(text: str, size: float = 1.0, w: int = 400) -> None:
    st.markdown(_RTL.format(size=size, w=w, text=html.escape(text or "")),
                unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def _load_candidates(mtime: float) -> list[dict]:
    if not CANDIDATES.exists():
        return []
    with CANDIDATES.open(encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _org_label(r: dict) -> str:
    return (r.get("org_name_yiddish") or "").strip() or (r.get("org_name") or "").strip()


def _record(r: dict, decision: str, reviewer: str, notes: str = "") -> dict:
    """One decision row. Reviewer is stamped on EVERY write — the activity log
    alone has proven too coarse to reconstruct who decided what."""
    return {
        "db_id": r["db_id"], "hub_id": r.get("hub_id") or "",
        "span": r.get("span") or "", "decision": decision,
        "tier": r.get("tier") or "", "pattern": r.get("pattern") or "",
        "org_name_yiddish": r.get("org_name_yiddish") or "",
        "person_heading": r.get("person_heading") or "",
        "person_db_ids": r.get("person_db_ids") or "",
        "reviewer": reviewer,
        "reviewed_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "notes": notes,
    }


def _save(records: list[dict]) -> None:
    if not records:
        st.warning("Nothing ticked — no decisions to save.")
        return
    ok = troupe_store.save_eponym_decisions(records)
    st.session_state["_eponym_saved"] = len(records)
    if ok:
        st.success(f"Saved {len(records)} decision(s).")
    else:
        st.warning(f"Saved {len(records)} locally, but the push failed — "
                   "they are on this machine and will sync on the next save.")
    st.rerun()


def render() -> None:
    st.title("Eponym links")
    st.caption(
        "An organization named after a person should link that person. "
        "Confirming here writes `edge_source=eponym` edges — the claim is "
        "**eponymy only**, not a role."
    )

    troupe_store.ensure_fresh()
    rows = _load_candidates(CANDIDATES.stat().st_mtime if CANDIDATES.exists() else 0.0)
    if not rows:
        st.error(
            f"No candidates found at `{CANDIDATES.relative_to(BASE)}`. "
            "Run `build_eponym_candidates.py` first."
        )
        return

    decided = troupe_store.load_eponym_decisions(
        troupe_store._mtime(troupe_store.EPONYM_DECISIONS))

    reviewer = st.session_state.get("reviewer", "")
    if not reviewer:
        st.info("Set your name in the sidebar before deciding — every decision "
                "is stamped with a reviewer.")

    def key_of(r: dict) -> str:
        return f"{r['db_id']}|{r.get('hub_id') or ''}"

    open_rows = [r for r in rows if key_of(r) not in decided]
    by_tier: dict[str, list[dict]] = {t: [] for t in TIER_ORDER}
    for r in open_rows:
        by_tier.setdefault(r.get("tier") or "unresolved", []).append(r)

    n_orgs = len({r["db_id"] for r in rows})
    c1, c2, c3 = st.columns(3)
    c1.metric("Candidate orgs", n_orgs)
    c2.metric("Decided", len(decided))
    c3.metric("Open candidates", len(open_rows))

    tier = st.radio(
        "Queue", TIER_ORDER, horizontal=True,
        format_func=lambda t: f"{TIER_LABEL[t]}  ({len(by_tier.get(t, []))})",
        key="eponym_tier",
    )
    queue = by_tier.get(tier, [])
    if not queue:
        st.success(f"Nothing open in **{TIER_LABEL[tier]}**.")
    elif tier == "unresolved":
        _render_unresolved(queue)
    elif tier == "low":
        _render_cards(queue, reviewer)
    else:
        _render_batch(queue, reviewer, tier)

    _render_corrections(rows, decided, reviewer)


def _render_batch(queue: list[dict], reviewer: str, tier: str) -> None:
    """High/medium: one table, one save. High is pre-ticked (surname + adjacent
    given name is checkable at a glance); medium is not."""
    default_on = tier == "high"
    st.markdown(
        f"**{len(queue)} candidates.** "
        + ("Pre-ticked — untick anything wrong, then confirm."
           if default_on else
           "Nothing pre-ticked — a unique surname match is not a sure one.")
    )
    page = 0
    if len(queue) > PAGE_SIZE:
        pages = (len(queue) + PAGE_SIZE - 1) // PAGE_SIZE
        page = st.number_input("Page", 1, pages, 1, key=f"ep_pg_{tier}") - 1
    chunk = queue[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

    table = [{
        "link": default_on,
        "org": _org_label(r),
        "span": r.get("span") or "",
        "person": r.get("person_heading") or "",
        "db_id": r["db_id"],
        "hub_id": r.get("hub_id") or "",
    } for r in chunk]

    edited = st.data_editor(
        table, hide_index=True, use_container_width=True,
        key=f"ep_ed_{tier}_{page}",
        column_config={
            "link": st.column_config.CheckboxColumn("link", width="small"),
            "org": st.column_config.TextColumn("organization", disabled=True),
            "span": st.column_config.TextColumn("name in org", disabled=True, width="small"),
            "person": st.column_config.TextColumn("person", disabled=True),
            "db_id": st.column_config.TextColumn("org", disabled=True, width="small"),
            "hub_id": st.column_config.TextColumn("hub", disabled=True, width="small"),
        },
    )

    by_key = {f"{r['db_id']}|{r.get('hub_id') or ''}": r for r in chunk}
    if st.button(f"Confirm ticked · {TIER_LABEL[tier]}", type="primary",
                 disabled=not reviewer, key=f"ep_save_{tier}_{page}"):
        recs = []
        for e in edited:
            src = by_key.get(f"{e['db_id']}|{e['hub_id']}")
            if not src:
                continue
            recs.append(_record(src, "CONFIRM" if e["link"] else "REJECT", reviewer))
        _save(recs)


def _render_cards(queue: list[dict], reviewer: str) -> None:
    """Low tier: group by org, show every candidate person. Picking IS deciding,
    so there is no default — פֿרידמאַן matches 15 hubs and a default would be a
    coin flip stamped with the reviewer's name."""
    orgs: dict[str, list[dict]] = {}
    for r in queue:
        orgs.setdefault(r["db_id"], []).append(r)
    ids = sorted(orgs, key=int)

    pages = (len(ids) + 9) // 10
    page = (st.number_input("Page", 1, max(pages, 1), 1, key="ep_pg_low") - 1) if pages > 1 else 0
    st.caption(f"{len(ids)} orgs with ambiguous or multi-name candidates.")

    for db_id in ids[page * 10:(page + 1) * 10]:
        group = orgs[db_id]
        head = group[0]
        with st.container(border=True):
            _rtl(_org_label(head), size=1.3, w=600)
            st.caption(
                f"db{db_id} · {head.get('pattern','')} · "
                f"{head.get('org_type') or 'no type'} · {head.get('notes','')}"
            )
            # One block per distinct name-span: a trio gets three questions.
            spans: dict[str, list[dict]] = {}
            for r in group:
                spans.setdefault(r.get("span") or "", []).append(r)

            picks: list[dict] = []
            for span, cands in spans.items():
                _rtl(f"— {span} —", size=1.0, w=600)
                labels = ["(skip)"] + [
                    f"{c.get('person_heading','')}  [hub {c.get('hub_id','')}]"
                    for c in cands
                ] + ["none of these"]
                choice = st.radio(
                    f"who is {span}?", labels, key=f"ep_r_{db_id}_{span}",
                    label_visibility="collapsed",
                )
                if choice == "none of these":
                    picks.append(_record(cands[0], "REJECT", reviewer,
                                         notes=f"no hub is {span}"))
                elif choice != "(skip)":
                    idx = labels.index(choice) - 1
                    picks.append(_record(cands[idx], "CONFIRM", reviewer))
                    # The others for this span are not this person.
                    for j, c in enumerate(cands):
                        if j != idx:
                            picks.append(_record(c, "REJECT", reviewer,
                                                 notes=f"{span} resolved to another hub"))

            if st.button("Save this org", key=f"ep_b_{db_id}",
                         type="primary", disabled=not reviewer):
                _save([p for p in picks])


def _render_unresolved(queue: list[dict]) -> None:
    st.markdown(
        f"**{len(queue)} spans matched no person.** Read-only: the fix is on the "
        "people side — a missing hub, or a spelling variant the surname index "
        "does not carry. Deciding these here would record a judgement about "
        "data that is simply absent."
    )
    st.dataframe(
        [{"db_id": r["db_id"], "organization": _org_label(r),
          "name in org": r.get("span") or "", "pattern": r.get("pattern") or ""}
         for r in queue[:400]],
        hide_index=True, use_container_width=True,
    )


def _render_corrections(rows: list[dict], decided: dict, reviewer: str) -> None:
    """Fix your own earlier calls without hunting back through the queue."""
    if not decided:
        return
    with st.expander(f"Correct saved decisions ({len(decided)})"):
        confirmed = [d for d in decided.values()
                     if (d.get("decision") or "").upper() == "CONFIRM"]
        st.caption(
            f"{len(confirmed)} CONFIRM → eponym edges once "
            "`apply_eponym_decisions.py --execute` runs. "
            "Flip a decision here and save to change that."
        )
        table = [{
            "decision": (d.get("decision") or "").upper(),
            "org": d.get("org_name_yiddish") or "",
            "span": d.get("span") or "",
            "person": d.get("person_heading") or "",
            "reviewer": d.get("reviewer") or "",
            "db_id": d.get("db_id") or "",
            "hub_id": d.get("hub_id") or "",
        } for d in sorted(decided.values(), key=lambda x: (x.get("reviewed_at") or ""), reverse=True)]
        edited = st.data_editor(
            table, hide_index=True, use_container_width=True, key="ep_corr",
            column_config={
                "decision": st.column_config.SelectboxColumn(
                    "decision", options=["CONFIRM", "REJECT"], width="small"),
                "org": st.column_config.TextColumn("organization", disabled=True),
                "span": st.column_config.TextColumn("name in org", disabled=True, width="small"),
                "person": st.column_config.TextColumn("person", disabled=True),
                "reviewer": st.column_config.TextColumn("by", disabled=True, width="small"),
                "db_id": st.column_config.TextColumn("org", disabled=True, width="small"),
                "hub_id": st.column_config.TextColumn("hub", disabled=True, width="small"),
            },
        )
        if st.button("Save corrections", key="ep_corr_save", disabled=not reviewer):
            recs = []
            for e in edited:
                prev = decided.get(f"{e['db_id']}|{e['hub_id']}")
                if not prev:
                    continue
                if (prev.get("decision") or "").upper() != e["decision"]:
                    recs.append({**prev, "decision": e["decision"],
                                 "reviewer": reviewer,
                                 "reviewed_at": _dt.datetime.now().isoformat(timespec="seconds"),
                                 "notes": (prev.get("notes") or "") + " [corrected]"})
            _save(recs)
