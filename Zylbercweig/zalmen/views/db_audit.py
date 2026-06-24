"""DB Audit — surface core_db rows whose constituent clusters look like a
false-equation merge (one wrong pair in the chain). The reviewer marks each
cluster KEEP_IN / REMOVE / CHECK; decisions go to db_audit_decisions.tsv and
are later applied surgically by apply_db_audit_decisions.py.

Input : Zylbercweig/organizations/db_audit_punchlist.tsv
        (produced by build_db_audit_punchlist.py — re-run to refresh)
Output: Zylbercweig/organizations/db_audit_decisions.tsv
        (committed via github_sync, like yidracor_flags)

UX shape — one st.container per flagged DB:
  - DB header (id, Yiddish, translit, type, min_pair_score, severity_boost)
  - Last-merge attribution from activity_log
  - List of aligned clusters sorted by worst-pair-score (most-suspicious first)
  - Per-cluster radio: KEEP_IN / REMOVE / CHECK
  - Notes textarea + Save button

Re-runs of the audit overwrite db_audit_punchlist.tsv; existing decisions
stay (composite key db_id+cluster_id), so a reviewer's work is preserved
across audit refreshes.
"""
from __future__ import annotations

import csv
import datetime as _dt
import fcntl
import json
import pathlib

import streamlit as st

from views.org_review import (
    CLUSTER_FILE,
    _open_url,
    load_samples,
    render_attestations,
)

# ── Paths ─────────────────────────────────────────────────────────────────────
ORG = pathlib.Path(__file__).resolve().parents[2] / "organizations"
PUNCHLIST = ORG / "db_audit_punchlist.tsv"
DECISIONS = ORG / "db_audit_decisions.tsv"
DECISIONS_REPO_PATH = "Zylbercweig/organizations/db_audit_decisions.tsv"

DECISION_HEADERS = [
    "db_id", "cluster_id", "decision", "reviewer_notes",
    "reviewer", "reviewed_at",
]


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data(show_spinner=False)
def load_punchlist(mtime: float) -> list[dict]:
    if not PUNCHLIST.exists():
        return []
    with open(PUNCHLIST, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    for r in rows:
        try:
            r["_clusters"] = json.loads(r.get("cluster_details_json", "") or "[]")
        except Exception:  # noqa: BLE001
            r["_clusters"] = []
    return rows


@st.cache_data(show_spinner=False)
def load_decisions(mtime: float) -> dict[tuple[str, str], dict]:
    """Composite-key dict: (db_id, cluster_id) → row."""
    out: dict[tuple[str, str], dict] = {}
    if not DECISIONS.exists():
        return out
    with open(DECISIONS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (r.get("db_id", ""), r.get("cluster_id", ""))
            out[key] = r
    return out


def save_decisions(records: list[dict]) -> None:
    """Upsert N decision rows under a single file-lock + one GitHub push."""
    if not records:
        return
    DECISIONS.parent.mkdir(parents=True, exist_ok=True)
    lock = DECISIONS.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            existing: dict[tuple[str, str], dict] = {}
            if DECISIONS.exists():
                with open(DECISIONS, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f, delimiter="\t"):
                        existing[(row.get("db_id", ""), row.get("cluster_id", ""))] = row
            for rec in records:
                existing[(rec["db_id"], rec["cluster_id"])] = rec
            with open(DECISIONS, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=DECISION_HEADERS, delimiter="\t")
                w.writeheader()
                for row in existing.values():
                    w.writerow({k: row.get(k, "") for k in DECISION_HEADERS})
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    try:
        from zalmen.github_sync import push_file_to_github
        ok = push_file_to_github(
            DECISIONS_REPO_PATH, DECISIONS,
            f"chore: db_audit decisions ({len(records)} rows)",
        )
    except Exception:  # noqa: BLE001
        ok = False
    if not ok:
        st.toast("⚠️ Saved locally but not pushed to GitHub (check secrets).", icon="⚠️")
    load_decisions.clear()


# ── Render helpers ────────────────────────────────────────────────────────────

_DECISION_OPTS = ["", "KEEP_IN", "REMOVE", "CHECK"]


def _render_db(idx: int, db: dict, decisions: dict[tuple[str, str], dict],
               reviewer: str, samples: dict[str, dict[str, list]]) -> None:
    db_id = db["db_id"]
    clusters = db.get("_clusters", [])
    sev = db.get("severity_boost", "") or ""

    # If a previous "Keep all in" click queued this DB, pre-fill each cluster's
    # radio state BEFORE the widgets render. Cleared after consumption so
    # subsequent manual changes stick.
    if st.session_state.pop(f"dba_setall_{db_id}", False):
        for c in clusters:
            st.session_state[f"dba_{db_id}_{c['cluster_id']}"] = "KEEP_IN"

    # Header line
    yi = db.get("db_name_yiddish", "") or "(no canonical Yiddish)"
    tr = db.get("db_name_translit", "") or ""
    head_bits = [
        f"**DB {db_id}** — :blue[{yi}]",
        f"_{tr}_" if tr else "",
    ]
    head_col, link_col = st.columns([4, 1])
    with head_col:
        st.markdown(" · ".join(b for b in head_bits if b))
    with link_col:
        st.link_button(
            "🗂 Full DB entry →",
            url=_open_url("Organization Cards", db_id),
            help="Open this DB in the Organization Cards view. Your in-flight "
                 "radio selections here persist across view-switches.",
            use_container_width=True,
        )

    # Stats line
    bits = [
        f"type: **{db.get('org_type','')}**",
        f"clusters: **{db.get('n_clusters','')}**",
        f"min_score: **{db.get('min_pair_score','')}**",
        f"weakest signal: `{db.get('weakest_pair_signal','')}`",
    ]
    if sev:
        bits.append(f"⚠ **{sev}**")
    st.caption(" · ".join(bits))

    # Attribution
    if db.get("last_merge_reviewer"):
        st.caption(
            f"last touched by **{db['last_merge_reviewer']}** on "
            f"{db.get('last_merge_ts','')} ({db.get('last_merge_action','')})"
        )

    st.caption("Decide whether each cluster belongs in this DB. Most-suspicious first.")

    # Per-cluster radio buttons
    for c in clusters:
        cid = c["cluster_id"]
        key_radio = f"dba_{db_id}_{cid}"
        prev = decisions.get((db_id, cid), {})
        prev_dec = prev.get("decision", "")
        try:
            idx_dec = _DECISION_OPTS.index(prev_dec)
        except ValueError:
            idx_dec = 0

        col_dec, col_info = st.columns([1, 4])
        with col_dec:
            st.radio(
                "decision", _DECISION_OPTS, index=idx_dec,
                key=key_radio, label_visibility="collapsed", horizontal=True,
            )
        with col_info:
            yi_c = c.get("canonical_yiddish", "") or "(no canonical)"
            tv = c.get("top_variant", "") or ""
            size = c.get("size", "")
            worst = c.get("worst_pair_score", "")
            best = c.get("best_pair_score", "")
            st.markdown(
                f"`{cid}` · size {size} · worst pair: **{worst}** · best pair: {best}  \n"
                f":blue[{yi_c}]"
                + (f"  \nvariant: {tv}" if tv and tv != yi_c else "")
            )
            n_samples = len((samples.get(cid) or {}).get("samples", []))
            mentions_label = (
                f"📜 Mentions ({n_samples})" if n_samples
                else "📜 Mentions (none in clustered TSV)"
            )
            with st.expander(mentions_label, expanded=False):
                if n_samples:
                    render_attestations({"cluster_id": cid}, samples)
                else:
                    st.caption(
                        "No source mentions found in organizations_clustered.tsv "
                        f"for `{cid}`."
                    )

    # Notes + Save + Keep-all-in shortcut
    notes_key = f"dba_notes_{db_id}"
    save_key = f"dba_save_{db_id}"
    keepall_key = f"dba_keepall_{db_id}"
    st.text_area("notes (applies to all decisions saved in this block)",
                 key=notes_key, height=68, label_visibility="collapsed",
                 placeholder="notes (optional)")
    btn_save, btn_keepall, _ = st.columns([2, 2, 3])
    with btn_keepall:
        if st.button(
            f"✅ Keep all in (DB {db_id})", key=keepall_key,
            help="Pre-fill every cluster in this DB with KEEP_IN. You still "
                 "need to click Save to commit.",
            use_container_width=True,
        ):
            st.session_state[f"dba_setall_{db_id}"] = True
            st.rerun()
    with btn_save:
        save_clicked = st.button(
            f"💾 Save decisions for DB {db_id}", key=save_key, type="primary",
            use_container_width=True,
        )
    if save_clicked:
        notes = st.session_state.get(notes_key, "") or ""
        ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        recs: list[dict] = []
        for c in clusters:
            cid = c["cluster_id"]
            choice = st.session_state.get(f"dba_{db_id}_{cid}", "") or ""
            if not choice:
                continue
            recs.append({
                "db_id": db_id,
                "cluster_id": cid,
                "decision": choice,
                "reviewer_notes": notes,
                "reviewer": reviewer,
                "reviewed_at": ts,
            })
        if not recs:
            st.toast("Nothing to save — no decisions picked.", icon="ℹ️")
        else:
            save_decisions(recs)
            try:
                from zalmen.activity_log import log_action
                for r in recs:
                    log_action(
                        "db_audit", "db_decision",
                        target_id=f"{r['db_id']}/{r['cluster_id']}",
                        decision=r["decision"],
                        note=notes,
                        push=False,  # avoid N pushes; the save already pushed the TSV
                    )
            except Exception:  # noqa: BLE001
                pass
            st.toast(f"✅ Saved {len(recs)} decisions for DB {db_id}", icon="✅")
            st.rerun()


# ── Main render ───────────────────────────────────────────────────────────────

def render() -> None:
    st.header("🩺 DB Audit — false-equation merges")
    st.caption(
        "Flagged DBs whose constituent clusters score below the alignment "
        "pipeline's MIN_SCORE (0.60). Each cluster's per-pair scores are "
        "shown so you can pin down the wrong addition. REMOVE = unalign from "
        "this DB (apply_db_audit_decisions.py executes); KEEP_IN = leave; "
        "CHECK = defer."
    )

    reviewer = st.session_state.get("reviewer", "")
    if not reviewer:
        st.warning("Pick your name in the sidebar to record decisions.")

    if not PUNCHLIST.exists():
        st.error(
            f"No punchlist yet. Run:\n\n"
            f"```bash\n"
            f"python Zylbercweig/organizations/build_db_audit_punchlist.py\n"
            f"```"
        )
        return

    punchlist = load_punchlist(_mtime(PUNCHLIST))
    decisions = load_decisions(_mtime(DECISIONS))
    samples = load_samples(_mtime(CLUSTER_FILE))

    if not punchlist:
        st.success("No DBs currently flagged. Re-run the audit to refresh.")
        return

    # Hide fully-resolved DBs: every cluster has a saved decision of KEEP_IN or
    # REMOVE (CHECK keeps the row visible — explicit "come back later" intent).
    # Decisions persist in db_audit_decisions.tsv across audit refreshes.
    def _is_resolved(r: dict) -> bool:
        cs = r.get("_clusters", [])
        if not cs:
            return False
        for c in cs:
            d = decisions.get((r["db_id"], c["cluster_id"]), {}).get("decision", "")
            if d not in ("KEEP_IN", "REMOVE"):
                return False
        return True

    n_resolved_total = sum(1 for r in punchlist if _is_resolved(r))

    # Filters
    col_f, col_s = st.columns([2, 1])
    with col_f:
        sev_only = st.checkbox(
            "Severity-boosted only (PRE_JUDGED_SPLIT etc.)",
            value=False, key="dba_filter_sev",
        )
    with col_s:
        max_show = st.number_input(
            "Show top N", min_value=10, max_value=500, value=50, step=10,
            key="dba_filter_n",
        )

    # Apply filters: hide resolved first, then severity, then top-N.
    rows = [r for r in punchlist if not _is_resolved(r)]
    if sev_only:
        rows = [r for r in rows if (r.get("severity_boost") or "").strip()]
    rows = rows[: int(max_show)]

    n_decided = sum(1 for r in rows if any(
        (r["db_id"], c["cluster_id"]) in decisions for c in r.get("_clusters", [])
    ))
    st.markdown(
        f"**{len(rows)} DBs shown** (of {len(punchlist)} flagged total; "
        f"{n_resolved_total} fully resolved and hidden). "
        f"{n_decided} of the shown DBs have at least one decision recorded."
    )

    for i, db in enumerate(rows):
        with st.container(border=True):
            _render_db(i, db, decisions, reviewer, samples)
