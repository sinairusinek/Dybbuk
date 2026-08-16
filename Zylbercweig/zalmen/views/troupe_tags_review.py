"""Troupe-tag draft review — a confidence-sorted queue over machine-drafted
troupe tags (build_troupe_tag_drafts.py), so Ruthie confirms rather than
fills 124 blanks by hand.

Flow per troupe:
  Accept  — write the draft tags to troupe_tags.tsv unchanged
  Save    — write whatever pills are currently ticked (edit-then-save)
  Reject  — record a rejection; the troupe stays untagged and drops out

Queue order (her choice): HIGH first (bulk-acceptable) → MEDIUM → BASE-ONLY
(needs her knowledge) → and a cross-cut group of the German-Jewish-flagged rows.

A second section, "Correct saved tags", is a flat table over everything already
written to troupe_tags.tsv — for fixing your own earlier calls without hunting
back through the queue. It is one st.data_editor rather than per-row widgets,
so reviewing 600 saved rows costs one widget instead of thousands.

Input : Zylbercweig/organizations/troupe_tags_draft.tsv
Output: troupe_tags.tsv        (accepted/edited tags — the production file,
                                reused via db_audit.save_troupe_tags)
        troupe_tag_review.tsv  (audit of accept/edit/reject per db_id)

Performance: mentions render lazily (one button → attestations for that troupe
only), and each tier paginates. Streamlit executes collapsed-expander bodies,
so we never put the heavy attestation render inside an always-open expander.
"""
from __future__ import annotations

import csv
import datetime as _dt
import fcntl
import pathlib

import streamlit as st

from views.org_review import CLUSTER_FILE, _open_url, load_samples, render_attestations
from views.db_audit import (
    TROUPE_TAGS,
    _TROUPE_TAG_OPTS,
    _split_tags,
    save_troupe_tags,
    load_troupe_tags,
)

ORG = pathlib.Path(__file__).resolve().parents[2] / "organizations"
DRAFTS = ORG / "troupe_tags_draft.tsv"
REVIEW = ORG / "troupe_tag_review.tsv"
REVIEW_REPO_PATH = "Zylbercweig/organizations/troupe_tag_review.tsv"

REVIEW_HEADERS = ["db_id", "status", "final_tags", "reviewer", "reviewed_at"]

# Draft confidence strings → display tier (order matters).
_TIERS = ["high", "medium", "low (base only)"]
_TIER_LABEL = {
    "high": "✅ High confidence — bulk-acceptable",
    "medium": "🟡 Medium confidence",
    "low (base only)": "⚪ Base only — needs your knowledge",
}


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data(show_spinner=False)
def load_drafts(mtime: float) -> list[dict]:
    if not DRAFTS.exists():
        return []
    with open(DRAFTS, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data(show_spinner=False)
def load_review(mtime: float) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not REVIEW.exists():
        return out
    with open(REVIEW, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out[r.get("db_id", "")] = r
    return out


def save_review(recs: list[dict]) -> None:
    """Upsert N review-audit rows (keyed db_id) under one lock + one push."""
    if not recs:
        return
    REVIEW.parent.mkdir(parents=True, exist_ok=True)
    lock = REVIEW.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            existing: dict[str, dict] = {}
            if REVIEW.exists():
                with open(REVIEW, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f, delimiter="\t"):
                        existing[row.get("db_id", "")] = row
            for rec in recs:
                existing[rec["db_id"]] = rec
            with open(REVIEW, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=REVIEW_HEADERS, delimiter="\t")
                w.writeheader()
                for row in existing.values():
                    w.writerow({k: row.get(k, "") for k in REVIEW_HEADERS})
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    try:
        from zalmen.github_sync import push_file_to_github
        push_file_to_github(REVIEW_REPO_PATH, REVIEW, "chore: troupe_tag_review")
    except Exception:  # noqa: BLE001
        pass
    load_review.clear()


def _commit_batch(items: list[tuple[str, list[str], str]], reviewer: str) -> None:
    """Commit N (db_id, tags, status) decisions with ONE troupe_tags push and
    ONE review-audit push — critical on Streamlit Cloud, where each push
    triggers a redeploy that resets sessions. status ∈ accept/edit/reject."""
    ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    tag_recs = [{
        "db_id": db_id, "tags": " | ".join(tags), "other_tags": "",
        "reviewer_notes": f"from draft ({status})",
        "reviewer": reviewer, "reviewed_at": ts,
    } for db_id, tags, status in items if tags]
    if tag_recs:
        save_troupe_tags(tag_recs)                       # one push
    save_review([{
        "db_id": db_id, "status": status,
        "final_tags": " | ".join(tags),
        "reviewer": reviewer, "reviewed_at": ts,
    } for db_id, tags, status in items])                 # one push
    try:
        from zalmen.activity_log import log_action
        for db_id, tags, status in items:
            log_action("troupe_review", status, target_id=db_id,
                       decision=" | ".join(tags), push=False)
    except Exception:  # noqa: BLE001
        pass


def _commit_tags(db_id: str, tags: list[str], reviewer: str, status: str) -> None:
    _commit_batch([(db_id, tags, status)], reviewer)


def _render_card(d: dict, reviewer: str, samples: dict) -> None:
    db_id = d["db_id"]
    draft_tags = [t for t in _split_tags(d.get("tags", "")) if t in _TROUPE_TAG_OPTS]

    name = d.get("name", "") or ""
    yi = d.get("name_yiddish", "") or ""
    head = f"**DB {db_id}** — :blue[{yi or '(no Yiddish)'}]" + (f" · _{name}_" if name else "")
    hcol, lcol = st.columns([4, 1])
    with hcol:
        st.markdown(head)
    with lcol:
        st.link_button("🗂 Full entry →", url=_open_url("Organization Cards", db_id),
                       use_container_width=True)

    if d.get("evidence"):
        st.caption("why these tags: " + d["evidence"])
    if d.get("review_flags"):
        # icon= must be a real emoji: "⚑" (U+2691) raises StreamlitAPIException
        # and takes the whole queue down on any flagged row.
        st.warning(d["review_flags"], icon="⚠️")

    # Pre-filled pills (editable). Default = the draft's tags.
    st.pills("Tags", _TROUPE_TAG_OPTS, selection_mode="multi",
             default=draft_tags, key=f"ttr_pills_{db_id}")

    # Lazy mentions — one button, renders attestations for THIS troupe only,
    # across all of its clusters. Built only when opened (Streamlit executes
    # collapsed-expander bodies, so we gate on a button, not an expander).
    cids = _split_tags(d.get("cluster_ids", ""))
    show_key = f"ttr_showment_{db_id}"
    if st.session_state.get(show_key):
        with st.expander("📜 Mentions", expanded=True):
            any_shown = False
            for cid in cids:
                if (samples.get(cid) or {}).get("samples"):
                    st.markdown(f"`{cid}`")
                    render_attestations({"cluster_id": cid}, samples)
                    any_shown = True
            if not any_shown:
                st.caption("No source mentions found for this troupe's clusters.")
    else:
        n = int(d.get("n_sents", "0") or 0)
        if n and st.button(f"📜 Show mentions ({n})", key=f"ttr_mbtn_{db_id}"):
            st.session_state[show_key] = True
            st.rerun()

    a, e, r, _ = st.columns([2, 2, 2, 3])
    with a:
        if st.button("✅ Accept draft", key=f"ttr_acc_{db_id}",
                     type="primary", use_container_width=True):
            _commit_tags(db_id, draft_tags, reviewer, "accept")
            st.toast(f"Accepted DB {db_id}", icon="✅")
            st.rerun()
    with e:
        if st.button("💾 Save edits", key=f"ttr_save_{db_id}",
                     use_container_width=True):
            picked = st.session_state.get(f"ttr_pills_{db_id}", []) or []
            _commit_tags(db_id, picked, reviewer, "edit")
            st.toast(f"Saved DB {db_id}", icon="💾")
            st.rerun()
    with r:
        if st.button("🚫 Reject", key=f"ttr_rej_{db_id}",
                     use_container_width=True):
            _commit_tags(db_id, [], reviewer, "reject")
            st.toast(f"Rejected DB {db_id} — left untagged", icon="🚫")
            st.rerun()


# ── Correct saved tags ────────────────────────────────────────────────────────

CORE_DB = ORG / "core_db.tsv"


@st.cache_data(show_spinner=False)
def load_core_names(mtime: float) -> dict[str, dict]:
    """db_id → {name, name_yiddish, org_type}, for labelling saved-tag rows."""
    out: dict[str, dict] = {}
    if not CORE_DB.exists():
        return out
    with open(CORE_DB, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out[r.get("db_id", "")] = {
                "name": r.get("name", ""),
                "name_yiddish": r.get("name_yiddish", ""),
                "org_type": r.get("org_type", ""),
            }
    return out


def _diff_and_validate(
    before_rows: list[dict], edited_rows: list[dict]
) -> tuple[list[dict], list[tuple[str, str]], list[str]]:
    """Which rows the reviewer actually changed, plus what's wrong with them.

    Returns (changed, unknown, emptied):
      changed — rows whose tags or other-tags differ from what was loaded
      unknown — (db_id, tag) for tags outside the vocabulary. Nothing downstream
                re-checks the tags column, so a typo saved here is invisible
                forever; the Save button is disabled while any exist.
      emptied — rows left with no tags at all. Legal, but they stay in
                troupe_tags.tsv as empty rows and so never return to the draft
                queue, which is worth warning about before it happens.
    """
    before = {r["db_id"]: r for r in before_rows}
    changed = [r for r in edited_rows
               if r["db_id"] in before
               and (r["tags"] != before[r["db_id"]]["tags"]
                    or r["other tags"] != before[r["db_id"]]["other tags"])]
    unknown = [(r["db_id"], t) for r in changed
               for t in _split_tags(r["tags"]) if t not in _TROUPE_TAG_OPTS]
    emptied = [r["db_id"] for r in changed
               if not _split_tags(r["tags"]) and not _split_tags(r["other tags"])]
    return changed, unknown, emptied


def _render_correct(reviewer: str) -> None:
    import pandas as pd

    tags_now = load_troupe_tags(_mtime(TROUPE_TAGS))
    if not tags_now:
        st.info("Nothing tagged yet — work the draft queue first.")
        return
    names = load_core_names(_mtime(CORE_DB))

    st.caption(
        "Everything saved to troupe_tags.tsv. Edit the **tags** / **other tags** "
        "cells directly — pipe-delimited, e.g. `Family Company | Operetta / Opera "
        "Company` — then Save. Only rows you actually changed are written."
    )

    rows = []
    for db_id, r in tags_now.items():
        meta = names.get(db_id, {})
        rows.append({
            "db_id": db_id,
            "name": meta.get("name", "") or meta.get("name_yiddish", ""),
            "yiddish": meta.get("name_yiddish", ""),
            "tags": " | ".join(_split_tags(r.get("tags", ""))),
            "other tags": " | ".join(_split_tags(r.get("other_tags", ""))),
            "by": r.get("reviewer", ""),
            "when": (r.get("reviewed_at", "") or "")[:10],
        })
    rows.sort(key=lambda x: (x["when"], x["db_id"]), reverse=True)
    df = pd.DataFrame(rows)

    # Filters — plain text/select, so the table stays one widget.
    c1, c2 = st.columns([3, 2])
    with c1:
        q = st.text_input("Filter by name or db_id", key="ttc_q",
                          placeholder="substring match")
    with c2:
        who = st.selectbox("Tagged by", ["everyone"] + sorted(
            {r["by"] for r in rows if r["by"]}), key="ttc_who")
    view = df
    if q:
        ql = q.strip().casefold()
        view = view[view.apply(
            lambda r: ql in str(r["name"]).casefold()
            or ql in str(r["yiddish"]).casefold()
            or ql in str(r["db_id"]).casefold(), axis=1)]
    if who != "everyone":
        view = view[view["by"] == who]

    st.caption(f"{len(view)} of {len(df)} tagged troupes shown. "
               f"Vocabulary: {' · '.join(_TROUPE_TAG_OPTS)}")

    edited = st.data_editor(
        view, key="ttc_editor", use_container_width=True, hide_index=True,
        column_config={
            "db_id": st.column_config.TextColumn("DB", disabled=True, width="small"),
            "name": st.column_config.TextColumn("name", disabled=True),
            "yiddish": st.column_config.TextColumn("Yiddish", disabled=True),
            "tags": st.column_config.TextColumn(
                "tags", help="Pipe-delimited. Must come from the vocabulary "
                             "listed above; anything else belongs in 'other tags'."),
            "other tags": st.column_config.TextColumn(
                "other tags", help="Free text, pipe-delimited — ideas the "
                                   "vocabulary doesn't cover yet."),
            "by": st.column_config.TextColumn("by", disabled=True, width="small"),
            "when": st.column_config.TextColumn("when", disabled=True, width="small"),
        },
    )

    changed, unknown, emptied = _diff_and_validate(
        view.to_dict("records"), edited.to_dict("records"))

    if not changed:
        st.caption("No edits yet.")
        return

    st.markdown(f"**{len(changed)} row(s) edited.**")
    if unknown:
        st.error(
            "Not in the vocabulary — fix these or move them to 'other tags':\n\n"
            + "\n".join(f"- DB {db}: `{t}`" for db, t in unknown),
            icon="⛔",
        )
    if emptied:
        st.warning(
            f"{len(emptied)} row(s) cleared of all tags (DB "
            f"{', '.join(emptied[:8])}{'…' if len(emptied) > 8 else ''}). They stay "
            "in troupe_tags.tsv as empty rows, so they will NOT come back in the "
            "draft queue.", icon="⚠️",
        )

    if st.button(f"💾 Save {len(changed)} correction(s)", type="primary",
                 disabled=bool(unknown), key="ttc_save"):
        ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        recs = [{
            "db_id": r["db_id"],
            "tags": " | ".join(_split_tags(r["tags"])),
            "other_tags": " | ".join(_split_tags(r["other tags"])),
            "reviewer_notes": "corrected in Correct-saved-tags",
            "reviewer": reviewer,
            "reviewed_at": ts,
        } for r in changed]
        save_troupe_tags(recs)                       # one lock + one push
        save_review([{
            "db_id": r["db_id"], "status": "correct",
            "final_tags": r["tags"],
            "reviewer": reviewer, "reviewed_at": ts,
        } for r in recs])
        try:
            from zalmen.activity_log import log_action
            for r in recs:
                log_action("troupe_review", "correct", target_id=r["db_id"],
                           decision=r["tags"], push=False)
        except Exception:  # noqa: BLE001
            pass
        st.toast(f"✅ Saved {len(recs)} corrections", icon="✅")
        st.rerun()


def render() -> None:
    st.header("🏷 Troupe-tag review")
    reviewer = st.session_state.get("reviewer", "")
    if not reviewer:
        st.warning("Pick your name in the sidebar to record decisions.")

    # Two jobs, one view. Routed off a radio rather than st.tabs: tabs execute
    # every tab's body on each rerun, so the draft queue would keep rebuilding
    # while you were correcting the table.
    section = st.radio(
        "Section", ["Draft queue", "Correct saved tags"],
        key="ttr_section", horizontal=True, label_visibility="collapsed",
    )
    if section == "Correct saved tags":
        _render_correct(reviewer)
        return

    if not DRAFTS.exists():
        st.error(
            "No draft file. Run:\n\n```bash\n"
            "python Zylbercweig/organizations/build_troupe_tag_drafts.py\n```"
        )
        return

    drafts = load_drafts(_mtime(DRAFTS))
    review = load_review(_mtime(REVIEW))
    tags_now = load_troupe_tags(_mtime(TROUPE_TAGS))
    samples = load_samples(_mtime(CLUSTER_FILE))

    # A draft is done once it's been reviewed (any status) or the DB is already
    # tagged in production. Those drop out of the queue.
    def _pending(d: dict) -> bool:
        db = d["db_id"]
        return db not in review and db not in tags_now

    pending = [d for d in drafts if _pending(d)]
    done = len(drafts) - len(pending)
    st.caption(
        f"{len(pending)} troupes left to review · {done} of {len(drafts)} done. "
        "Accept keeps the draft as-is; Save writes your edited pills; Reject "
        "leaves the troupe untagged."
    )

    only_flagged = st.checkbox("⚑ Show only German-Jewish-flagged rows",
                               value=False, key="ttr_only_flagged")
    if only_flagged:
        pending = [d for d in pending if d.get("review_flags")]

    per_tier = st.number_input("Show top N per tier", min_value=5, max_value=200,
                               value=25, step=5, key="ttr_pagesize")

    for tier in _TIERS:
        rows = [d for d in pending if d.get("confidence", "") == tier]
        if not rows:
            continue
        st.subheader(f"{_TIER_LABEL[tier]} · {len(rows)}")

        # Bulk accept for a whole tier (only meaningful for high, but offered
        # everywhere; it accepts each row's draft tags unchanged).
        if st.button(f"✅ Accept all shown in this tier ({min(len(rows), int(per_tier))})",
                     key=f"ttr_bulk_{tier}"):
            batch = [
                (d["db_id"],
                 [t for t in _split_tags(d.get("tags", "")) if t in _TROUPE_TAG_OPTS],
                 "accept")
                for d in rows[: int(per_tier)]
            ]
            _commit_batch(batch, reviewer)   # one troupe_tags push + one review push
            st.toast(f"Accepted {len(batch)} in {tier}", icon="✅")
            st.rerun()

        for d in rows[: int(per_tier)]:
            with st.container(border=True):
                _render_card(d, reviewer, samples)
