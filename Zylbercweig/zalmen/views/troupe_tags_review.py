"""Troupe-tag draft review — a confidence-sorted queue over machine-drafted
troupe tags (build_troupe_tag_drafts.py), so Ruthie confirms rather than
fills 124 blanks by hand.

Flow per troupe: a two-panel card — draft tags (editable pills) on the left,
the scrollable lexicon text naming the troupe on the right — and one **Save
edits** button. Saving writes the ticked pills to troupe_tags.tsv and the
troupe leaves the queue (it is then "already tagged", correctable in the second
section). There is no separate accept/reject: accepting the draft unchanged is
just Save without editing.

A radio picks which confidence group to work — HIGH / MEDIUM / BASE-ONLY, plus
a cross-cut "German-Jewish?" group of the flagged rows — so the lower-confidence
tiers are reachable directly, not only after scrolling past the high ones.

A second section, "Correct saved tags", is a flat table over everything already
written to troupe_tags.tsv — for fixing your own earlier calls without hunting
back through the queue. It is one st.data_editor rather than per-row widgets,
so reviewing 600 saved rows costs one widget instead of thousands.

Input : Zylbercweig/organizations/troupe_tags_draft.tsv
Output: troupe_tags.tsv        (accepted/edited tags — the production file)
        troupe_tag_review.tsv  (audit of accept/edit/reject per db_id)

Persistence rides `troupe_store`: both output files live on a data-only branch
that Streamlit Cloud does not deploy, so SAVING NO LONGER RESTARTS THE SERVER —
the "app jumps mid-work" failure came from every save redeploying the app. The
store fetches and merges that branch at boot, so a restart (a code deploy, or a
save from a view still on the deployed branch) comes back with every saved tag.

Performance: mentions render lazily (one button → attestations for that troupe
only), and each tier paginates. Streamlit executes collapsed-expander bodies,
so we never put the heavy attestation render inside an always-open expander.
"""
from __future__ import annotations

import csv
import datetime as _dt
import pathlib

import streamlit as st

from views.org_review import CLUSTER_FILE, load_samples
import troupe_store
from troupe_store import (
    TROUPE_TAGS,
    TAG_REVIEW as REVIEW,
    _TROUPE_TAG_OPTS,
    _split_tags,
    save_troupe_tags,
    load_troupe_tags,
    save_tag_review,
    load_tag_review,
)
from lexicon import JSON_TO_XML, get_entry_text
import mention_removals

ORG = pathlib.Path(__file__).resolve().parents[2] / "organizations"
DRAFTS = ORG / "troupe_tags_draft.tsv"

# Draft confidence strings → display tier (order matters).
_TIERS = ["high", "medium", "low (base only)"]
_TIER_LABEL = {
    "high": "✅ High confidence — bulk-acceptable",
    "medium": "🟡 Medium confidence",
    "low (base only)": "⚪ Base only — needs your knowledge",
}


# ── surviving a redeploy ──────────────────────────────────────────────────────
# Saves in THIS view no longer restart the server (they push to the data branch,
# see troupe_store) — but the server still restarts when code is deployed or
# when a view still on the deployed branch saves, and that drops the session.
# `?view=` brings the RA back to THIS view; mirroring the section and the
# confidence tier in the URL means the reconnect replays the exact group she was
# working instead of the top of the first tier ("it jumped").
#
# Slugs, not the raw labels: the tier keys carry spaces and parens, and the radio
# INDEX is not stable — inserting a tier would silently move everyone.
_TIER_SLUG = {
    "high": "high", "medium": "medium", "low (base only)": "base",
    "flagged": "flagged", "notext": "notext",
}
_SLUG_TIER = {v: k for k, v in _TIER_SLUG.items()}
_SECTION_SLUG = {"Draft queue": "queue", "Correct saved tags": "correct"}
_GROUP_KEYS = list(_TIERS) + ["flagged", "notext"]
_SLUG_SECTION = {v: k for k, v in _SECTION_SLUG.items()}


def _restore_from_url(group_keys: list[str]) -> None:
    """Apply ?section= / ?tier= to the widgets' session_state, BEFORE they are
    created — Streamlit takes a widget's value from session_state only if the key
    is set first.

    Each param re-applies only when its value changes, exactly as app.py does for
    `?view=`: a stale URL must not overwrite the pick the reviewer just made on
    the very rerun where her on_change had already moved it."""
    qp = st.query_params
    sec = _SLUG_SECTION.get(qp.get("section", ""))
    if sec and sec != st.session_state.get("_ttr_section_applied"):
        st.session_state["ttr_section"] = sec
        st.session_state["_ttr_section_applied"] = sec
    tier = _SLUG_TIER.get(qp.get("tier", ""))
    if tier and tier in group_keys and tier != st.session_state.get("_ttr_tier_applied"):
        st.session_state["ttr_group"] = group_keys.index(tier)
        st.session_state["_ttr_tier_applied"] = tier


def _mirror_to_url(section: str, tier: str | None) -> None:
    """Keep the URL in step with where she is. Assigning to query_params only
    sends a page_info_changed message — it does not rerun — so this is safe to
    call on every render."""
    slug = _SECTION_SLUG.get(section)
    if slug and st.query_params.get("section") != slug:
        st.query_params["section"] = slug
    tslug = _TIER_SLUG.get(tier or "")
    if tslug and st.query_params.get("tier") != tslug:
        st.query_params["tier"] = tslug


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data(show_spinner=False)
def load_drafts(mtime: float) -> list[dict]:
    if not DRAFTS.exists():
        return []
    with open(DRAFTS, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _commit_batch(items: list[tuple[str, list[str], str, str]], reviewer: str) -> None:
    """Commit N (db_id, tags, status, comment) decisions in ONE GitHub commit
    to the DATA branch — persistence without a server restart (troupe_store).
    status ∈ accept/edit/reject.

    A blank comment never erases one already on the row: the card's box starts
    empty on a fresh render, so clearing it must be deliberate (done from the
    Correct-saved-tags table, which shows the current text). A row is written
    when it has tags OR a comment — a comment-only row is a legitimate way to
    park a question about a troupe you can't yet tag."""
    ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    prev = load_troupe_tags(_mtime(TROUPE_TAGS))
    tag_recs = [{
        "db_id": db_id, "tags": " | ".join(tags), "other_tags": "",
        "comment": comment or (prev.get(db_id, {}).get("comment", "") or ""),
        "reviewer_notes": f"from draft ({status})",
        "reviewer": reviewer, "reviewed_at": ts,
    } for db_id, tags, status, comment in items if tags or comment]
    if tag_recs:
        save_troupe_tags(tag_recs, push=False)
    save_tag_review([{
        "db_id": db_id, "status": status,
        "final_tags": " | ".join(tags),
        "reviewer": reviewer, "reviewed_at": ts,
    } for db_id, tags, status, _comment in items], push=False)

    # One commit for both files, straight to the data branch — no restart.
    repo_paths = [troupe_store.TAG_REVIEW_REPO_PATH]
    if tag_recs:
        repo_paths.insert(0, troupe_store.TROUPE_TAGS_REPO_PATH)
    if not troupe_store.push_batch(
            repo_paths, f"chore: troupe tags ({len(items)} reviewed)"):
        st.toast("⚠️ Saved locally but not pushed to GitHub (check secrets).",
                 icon="⚠️")
    try:
        from zalmen.activity_log import log_action
        for db_id, tags, status, _comment in items:
            log_action("troupe_review", status, target_id=db_id,
                       decision=" | ".join(tags), push=False)
    except Exception:  # noqa: BLE001
        pass


def _commit_tags(db_id: str, tags: list[str], reviewer: str, status: str,
                 comment: str = "") -> None:
    _commit_batch([(db_id, tags, status, comment)], reviewer)


def _mention_lines(d: dict, samples: dict) -> list[tuple[str, str, str, str]]:
    """Unique (heading, sentence, file, xml_id) mention lines across this
    troupe's clusters. file/xml_id address the lexicon entry the sentence came
    from, so the panel can offer to open its full text."""
    lines, seen = [], set()
    for cid in _split_tags(d.get("cluster_ids", "")):
        for head, sent, fle, xid in (samples.get(cid) or {}).get("samples", []):
            key = (head, sent)
            if sent and key not in seen:
                seen.add(key)
                lines.append((head, sent, fle, xid))
    return lines


def _render_text_panel(d: dict, samples: dict, db_id: str = "") -> None:
    """Right panel: the lexicon lines that name this troupe, in a fixed-height
    scrollable box (st.container(height=...) gives the scroll)."""
    lines = _mention_lines(d, samples)
    box = st.container(height=360, border=True)
    with box:
        if not lines:
            st.caption("No source mentions found for this troupe's clusters.")
        else:
            if d.get("text_source", "") == "name-match":
                st.caption("⚠️ recovered by name match — core_db does not link "
                           "this troupe to the cluster below; verify it is the "
                           "same troupe.")
            st.caption(f"{len(lines)} lexicon mention(s) naming this troupe:")
            for i, (head, sent, _f, _x) in enumerate(lines[:80], start=1):
                st.markdown(
                    f"<div dir='rtl' style='margin-bottom:.55em; line-height:1.5'>"
                    f"<b>{i}. {head or '(no heading)'}</b> — {sent}</div>",
                    unsafe_allow_html=True,
                )
            if len(lines) > 80:
                st.caption(f"… and {len(lines) - 80} more")
    # Outside the scroll box, so the opener stays reachable without scrolling.
    _render_entry_opener(db_id, lines[:80])


def _draft_tags(d: dict) -> list[str]:
    """The drafted tags for a row, vocabulary-filtered — what a card shows before
    the reviewer touches it."""
    return [t for t in _split_tags(d.get("tags", "")) if t in _TROUPE_TAG_OPTS]


def _split_page(rows: list[dict], picks: dict[str, tuple[list[str] | None, str]]
                ) -> tuple[list[tuple], list[tuple]]:
    """Sort a page of cards into (touched, untouched) decision tuples.

    `picks` is db_id → (tags picked in this session or None if the card has not
    rendered yet, comment). A card counts as touched when its tags differ from
    the draft or it carries a comment — that is the reviewer having said
    something. An untouched card is an unreviewed draft, and accepting it is a
    separate, explicit act.

    Tuples are the (db_id, tags, status, comment) shape `_commit_batch` takes,
    with status 'edit' for touched and 'accept' for untouched.
    """
    touched, untouched = [], []
    for d in rows:
        db_id = d["db_id"]
        picked, comment = picks.get(db_id, (None, ""))
        drafted = _draft_tags(d)
        if picked is None:
            untouched.append((db_id, drafted, "accept", ""))
            continue
        if set(picked) != set(drafted) or comment.strip():
            touched.append((db_id, picked, "edit", comment.strip()))
        else:
            untouched.append((db_id, drafted, "accept", ""))
    return touched, untouched


def _forget_cards(db_ids: list[str]) -> None:
    """Drop a saved card's widget state. The row leaves the queue, so its keys
    would otherwise sit in session_state for the rest of the session."""
    for db_id in db_ids:
        for k in (f"ttr_pills_{db_id}", f"ttr_comment_{db_id}", f"ttr_entry_{db_id}"):
            st.session_state.pop(k, None)


def _render_entry_opener(db_id: str, lines: list[tuple[str, str, str, str]]) -> None:
    """Optional: open the FULL lexicon entry behind one of the mentions above.

    A selectbox rather than a link or an expander per mention, on purpose. An
    expander body executes even while collapsed, so 80 of them would parse a
    volume per mention on every rerun (the bug this view's docstring warns
    about); a button per mention is 80 widgets per card, and there can be 200
    cards on a page. This is one widget, and `get_entry_text` runs only for the
    entry actually chosen — nothing is read until the reviewer asks."""
    openable = [(i, h, f, x) for i, (h, _s, f, x) in enumerate(lines, start=1)
                if f and x]
    if not openable:
        return
    pick = st.selectbox(
        "Open the full entry behind a mention (optional)",
        options=range(len(openable)), index=None,
        format_func=lambda j: f"{openable[j][0]}. {openable[j][1] or '(no heading)'}",
        placeholder="— pick a mention —", key=f"ttr_entry_{db_id}",
        label_visibility="collapsed",
    )
    if pick is None:
        return
    _n, head, fle, xid = openable[pick]
    text = get_entry_text(fle, xid)
    if not text:
        st.caption(f"Entry not found in XML ({JSON_TO_XML.get(fle, fle)}).")
        return
    st.caption(f"Full entry — {head or '(no heading)'} · {xid}")
    with st.container(height=300, border=True):
        st.markdown(
            f"<div dir='rtl' style='font-size:.9em; white-space:pre-wrap; "
            f"line-height:1.6'>{text}</div>", unsafe_allow_html=True,
        )


def _render_card(d: dict, reviewer: str, samples: dict) -> None:
    db_id = d["db_id"]
    draft_tags = _draft_tags(d)
    name = d.get("name", "") or ""
    yi = d.get("name_yiddish", "") or ""
    st.markdown(
        f"**DB {db_id}** — :blue[{yi or '(no Yiddish)'}]"
        + (f" · _{name}_" if name else "")
    )

    # Two panels: tags on the left, scrollable lexicon text on the right.
    left, right = st.columns([1, 1])
    with left:
        if d.get("evidence"):
            st.caption("why these tags: " + d["evidence"])
        if d.get("review_flags"):
            # icon= must be a real emoji: "⚑" (U+2691) raises StreamlitAPIException.
            st.warning(d["review_flags"], icon="⚠️")
        st.pills("Tags", _TROUPE_TAG_OPTS, selection_mode="multi",
                 default=draft_tags, key=f"ttr_pills_{db_id}")
        st.text_input(
            "Comment (optional)", key=f"ttr_comment_{db_id}",
            placeholder="a doubt, a source, anything the tags can't say",
            help="Free text, saved with the row. Leave blank and nothing is "
                 "written — an existing comment is kept, not erased.",
        )
        if st.button("💾 Save edits", key=f"ttr_save_{db_id}", type="primary",
                     use_container_width=True):
            picked = st.session_state.get(f"ttr_pills_{db_id}", []) or []
            note = (st.session_state.get(f"ttr_comment_{db_id}", "") or "").strip()
            _commit_tags(db_id, picked, reviewer, "edit", note)
            st.toast(f"Saved DB {db_id} — moved to already-tagged", icon="💾")
            st.rerun()
    with right:
        _render_text_panel(d, samples, db_id)


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
      emptied — rows left with no tags AND no comment. Legal, but they stay in
                troupe_tags.tsv as empty rows and so never return to the draft
                queue, which is worth warning about before it happens. A row
                cleared of tags but carrying a comment is deliberate, not empty.
    """
    before = {r["db_id"]: r for r in before_rows}
    changed = [r for r in edited_rows
               if r["db_id"] in before
               and (r["tags"] != before[r["db_id"]]["tags"]
                    or r["other tags"] != before[r["db_id"]]["other tags"]
                    # .get: the comment column post-dates callers/tests that
                    # pass two-column rows.
                    or r.get("comment", "") != before[r["db_id"]].get("comment", ""))]
    unknown = [(r["db_id"], t) for r in changed
               for t in _split_tags(r["tags"]) if t not in _TROUPE_TAG_OPTS]
    emptied = [r["db_id"] for r in changed
               if not _split_tags(r["tags"]) and not _split_tags(r["other tags"])
               and not (r.get("comment", "") or "").strip()]
    return changed, unknown, emptied


def _render_correct(reviewer: str) -> None:
    import pandas as pd

    tags_now = load_troupe_tags(_mtime(TROUPE_TAGS))
    if not tags_now:
        st.info("Nothing tagged yet — work the draft queue first.")
        return
    names = load_core_names(_mtime(CORE_DB))

    st.caption(
        "Everything saved to troupe_tags.tsv. Edit the **tags** / **other tags** / **comment** "
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
            "comment": r.get("comment", "") or "",
            "by": r.get("reviewer", ""),
            "when": (r.get("reviewed_at", "") or "")[:10],
        })
    rows.sort(key=lambda x: (x["when"], x["db_id"]), reverse=True)
    df = pd.DataFrame(rows)

    # Filters — plain text/select, so the table stays one widget.
    c1, c2 = st.columns([3, 2])
    with c1:
        q = st.text_input("Filter by name, comment or db_id", key="ttc_q",
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
            or ql in str(r["comment"]).casefold()
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
            "comment": st.column_config.TextColumn(
                "comment", help="Optional free text about this troupe. This is "
                                "the only place an existing comment can be "
                                "cleared — the draft card never erases one."),
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
            "comment": (r.get("comment", "") or "").strip(),
            "reviewer_notes": "corrected in Correct-saved-tags",
            "reviewer": reviewer,
            "reviewed_at": ts,
        } for r in changed]
        # Same one-commit shape as the draft queue, straight to the data branch.
        save_troupe_tags(recs, push=False)
        save_tag_review([{
            "db_id": r["db_id"], "status": "correct",
            "final_tags": r["tags"],
            "reviewer": reviewer, "reviewed_at": ts,
        } for r in recs], push=False)
        if not troupe_store.push_batch(
            [troupe_store.TROUPE_TAGS_REPO_PATH, troupe_store.TAG_REVIEW_REPO_PATH],
            f"chore: troupe tag corrections ({len(recs)} rows)",
        ):
            st.toast("⚠️ Saved locally but not pushed to GitHub.", icon="⚠️")
        try:
            from zalmen.activity_log import log_action
            for r in recs:
                log_action("troupe_review", "correct", target_id=r["db_id"],
                           decision=r["tags"], push=False)
        except Exception:  # noqa: BLE001
            pass
        st.toast(f"✅ Saved {len(recs)} corrections", icon="✅")
        st.rerun()


def _render_page_save(shown: list[dict], reviewer: str) -> None:
    """Save the whole page in ONE commit to the data branch — no restart, and
    one commit instead of one per card. (Batching predates the branch split; it
    stays because 20 cards as one commit is also one API round-trip, one toast,
    and one legible entry in the branch history.)

    Rendered ABOVE the cards on purpose. Streamlit runs the script top-down, so
    it reads the pills and comment boxes from session_state as they stood at the
    END of the previous rerun — which is exactly the reviewer's current picks —
    and she does not have to scroll to the bottom to save.

    Deliberately two buttons. Saving what she edited is safe and needs no
    ceremony; accepting drafts she never touched writes decisions about rows
    nobody read, so it is separate and gated behind a checkbox.
    """
    picks = {}
    for d in shown:
        db_id = d["db_id"]
        pills_key = f"ttr_pills_{db_id}"
        picked = (list(st.session_state[pills_key])
                  if pills_key in st.session_state else None)
        picks[db_id] = (picked, st.session_state.get(f"ttr_comment_{db_id}", "") or "")
    touched, untouched = _split_page(shown, picks)

    def _save(items: list[tuple], label: str) -> None:
        _commit_batch(items, reviewer)
        _forget_cards([i[0] for i in items])
        st.toast(f"Saved {len(items)} {label} in one commit", icon="💾")
        st.rerun()

    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button(
            f"💾 Save the {len(touched)} card(s) I changed on this page",
            key="ttr_save_page", type="primary", use_container_width=True,
            disabled=not touched,
            help="Saves every card you changed on this page in one go.",
        ):
            _save(touched, "cards")
    with c2:
        ok = st.checkbox(
            f"also accept the {len(untouched)} untouched draft(s) as-is",
            key="ttr_accept_untouched", disabled=not untouched,
            help="Writes the drafted tags for cards you did not change. Only "
                 "tick this once you have actually read them.",
        )
        if st.button("✅ Accept untouched drafts", key="ttr_accept_page",
                     use_container_width=True, disabled=not (ok and untouched)):
            _save(untouched, "drafts as-is")

    if touched:
        st.caption("Changed on this page: DB "
                   + ", ".join(i[0] for i in touched[:12])
                   + ("…" if len(touched) > 12 else ""))


def render() -> None:
    st.header("🏷 Troupe-tag review")

    # Pull the data branch into the local files (once per server process) BEFORE
    # anything reads them — after a restart this is what brings back every tag
    # saved since the last code deploy. Reads still work when it fails (checkout
    # snapshot), and saves re-attempt the merge themselves, so this only warns.
    if not troupe_store.ensure_fresh() and troupe_store.has_github():
        st.warning(
            "Couldn't refresh tags from the data branch — showing the deployed "
            "snapshot, which may be missing recent saves. Saving is still safe "
            "(each save re-merges); reload to retry.", icon="🔄",
        )

    reviewer = st.session_state.get("reviewer", "")
    if not reviewer:
        st.warning("Pick your name in the sidebar to record decisions.")

    # Two jobs, one view. Routed off a radio rather than st.tabs: tabs execute
    # every tab's body on each rerun, so the draft queue would keep rebuilding
    # while you were correcting the table.
    _restore_from_url(_GROUP_KEYS)
    section = st.radio(
        "Section", ["Draft queue", "Correct saved tags"],
        key="ttr_section", horizontal=True, label_visibility="collapsed",
    )
    if section == "Correct saved tags":
        _mirror_to_url(section, None)
        _render_correct(reviewer)
        return

    if not DRAFTS.exists():
        st.error(
            "No draft file. Run:\n\n```bash\n"
            "python Zylbercweig/organizations/build_troupe_tag_drafts.py\n```"
        )
        return

    drafts = load_drafts(_mtime(DRAFTS))
    review = load_tag_review(_mtime(REVIEW))
    tags_now = load_troupe_tags(_mtime(TROUPE_TAGS))
    samples = load_samples(mention_removals.cluster_cache_key(CLUSTER_FILE))

    # A draft is done once it's been reviewed (any status) or the DB is already
    # tagged in production. Those drop out of the queue.
    def _pending(d: dict) -> bool:
        db = d["db_id"]
        return db not in review and db not in tags_now

    pending = [d for d in drafts if _pending(d)]
    done = len(drafts) - len(pending)
    st.caption(
        f"{len(pending)} troupes left to review · {done} of {len(drafts)} done. "
        "Adjust the tags on the left, read the lexicon text on the right, then "
        "click **Save edits** — the troupe moves to “already tagged” (see the "
        "*Correct saved tags* section to revisit it)."
    )

    # Confidence-group picker — this is where you choose which tier to work,
    # including the lower-confidence ones. "German-Jewish?" is the flagged
    # cross-cut; "No lexicon text" are troupes core_db never linked to any
    # cluster, so there is nothing to read — held apart from the real tiers.
    def _no_text(d: dict) -> bool:
        return d.get("text_source", "") == "none"

    group_keys = _GROUP_KEYS
    group_label = {
        "high": "✅ High", "medium": "🟡 Medium",
        "low (base only)": "⚪ Base only",
        "flagged": "⚠️ German-Jewish?",
        "notext": "📭 No lexicon text",
    }

    def _count(k: str) -> int:
        if k == "notext":
            return sum(1 for d in pending if _no_text(d))
        if k == "flagged":
            return sum(1 for d in pending if d.get("review_flags") and not _no_text(d))
        return sum(1 for d in pending if d.get("confidence", "") == k and not _no_text(d))

    idx = st.radio(
        "Confidence group",
        range(len(group_keys)),
        format_func=lambda i: f"{group_label[group_keys[i]]} · {_count(group_keys[i])}",
        horizontal=True, key="ttr_group",
    )
    chosen = group_keys[idx]
    _mirror_to_url(section, chosen)
    if chosen == "notext":
        rows = [d for d in pending if _no_text(d)]
        st.info(
            "These troupes aren't linked to any cluster in core_db, so there's no "
            "lexicon text to show. Tag from the name if you recognise it, or Save "
            "empty to skip. (This reflects a core_db linkage gap, not a tab bug.)",
            icon="📭",
        )
    elif chosen == "flagged":
        rows = [d for d in pending if d.get("review_flags") and not _no_text(d)]
    else:
        rows = [d for d in pending if d.get("confidence", "") == chosen and not _no_text(d)]

    if not rows:
        st.success("Nothing left in this group. 🎉")
        return

    page = st.number_input("Show top N", min_value=5, max_value=200,
                           value=20, step=5, key="ttr_pagesize")
    shown = rows[: int(page)]
    st.caption(f"Showing {len(shown)} of {len(rows)} in this group.")

    _render_page_save(shown, reviewer)

    for d in shown:
        with st.container(border=True):
            _render_card(d, reviewer, samples)
