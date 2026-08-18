"""YiDraCor annotation-flags review.

Per-edition annotation pipeline (TEI ← Transkribus PAGE-XML) emits flags for an
RA to resolve: untyped <stage> spans, speakers missing a role xml:id, missing
cast members, collective speakers, legacy tags/attrs. This view shows each flag
with a cropped image of the exact text line, the Yiddish text, an approximate
transliteration, and a deep-link to the page in Transkribus — and records the
RA's decision back to the repo (so the pipeline can apply it).

Input  : YiDraCor/data/review/annotation_flags_enriched.tsv  (make_flag_crops.py)
Output : YiDraCor/data/review/annotation_flags_decisions.tsv (committed via github_sync)
"""
from __future__ import annotations

import csv
import datetime as _dt
import fcntl
import json
import pathlib
import sys

import streamlit as st
from atomic_io import atomic_write

# ── Paths ─────────────────────────────────────────────────────────────────────
YIDRACOR = pathlib.Path(__file__).resolve().parents[3] / "YiDraCor"
ENRICHED = YIDRACOR / "data" / "review" / "annotation_flags_enriched.tsv"
DECISIONS = YIDRACOR / "data" / "review" / "annotation_flags_decisions.tsv"
DECISIONS_REPO_PATH = "YiDraCor/data/review/annotation_flags_decisions.tsv"

for _p in (str(YIDRACOR / "code"), str(YIDRACOR / "code" / "annotation")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Pull the canonical tag vocabularies + edition resolver from the pipeline so the
# dropdowns never drift from schema.py.
try:
    from annotation.schema import STAGE_TYPES, HEADING_TYPES  # type: ignore
except Exception:  # noqa: BLE001
    STAGE_TYPES = {"setting", "entrance", "exit", "business", "delivery", "location", "costume", "novelistic"}
    HEADING_TYPES = {"act", "scene"}
try:
    from make_flag_crops import load_editions, resolve_edition  # type: ignore
except Exception:  # noqa: BLE001
    load_editions = resolve_edition = None  # type: ignore

DECISION_HEADERS = [
    "row_key", "edition", "page", "line", "category", "owner",
    "decision", "role_id", "note", "reviewer", "updated",
]


# ── Data load ─────────────────────────────────────────────────────────────────
def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data(show_spinner=False)
def load_flags(mtime: float) -> list[dict]:
    if not ENRICHED.exists():
        return []
    with open(ENRICHED, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data(show_spinner=False)
def load_decisions(mtime: float) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not DECISIONS.exists():
        return out
    with open(DECISIONS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out[r["row_key"]] = r
    return out


@st.cache_data(show_spinner=False)
def cast_roles(folder: str) -> list[str]:
    path = YIDRACOR / "data" / folder / "cast_dict.json"
    if not path.exists():
        return []
    try:
        d = json.load(open(path, encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []
    return sorted((d.get("roles") or {}).keys())


def row_key(r: dict) -> str:
    return "|".join([r.get("edition", ""), r.get("page(s)", ""),
                     r.get("line_id / count", ""), r.get("category", "")])


def save_decision(rec: dict) -> None:
    """Upsert one decision into the TSV (file-locked) and push to GitHub."""
    DECISIONS.parent.mkdir(parents=True, exist_ok=True)
    lock = DECISIONS.with_suffix(".lock")
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            existing: dict[str, dict] = {}
            if DECISIONS.exists():
                with open(DECISIONS, newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f, delimiter="\t"):
                        existing[row["row_key"]] = row
            existing[rec["row_key"]] = rec
            with atomic_write(DECISIONS) as f:
                w = csv.DictWriter(f, fieldnames=DECISION_HEADERS, delimiter="\t")
                w.writeheader()
                for row in existing.values():
                    w.writerow({k: row.get(k, "") for k in DECISION_HEADERS})
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    try:
        from zalmen.github_sync import push_file_to_github
        ok = push_file_to_github(DECISIONS_REPO_PATH, DECISIONS, "chore: YiDraCor flag decision")
    except Exception:  # noqa: BLE001
        ok = False
    if not ok:
        st.toast("⚠️ Saved locally but not pushed to GitHub (check secrets).", icon="⚠️")
    load_decisions.clear()


# ── Per-category decision control ─────────────────────────────────────────────
def decision_widget(r: dict, key: str, prev: dict, roles: list[str]):
    cat = r.get("category", "")
    owner = r.get("owner", "")
    pv = prev.get("decision", "")
    role_default = prev.get("role_id", "")

    if cat == "untyped stage":
        opts = [""] + sorted(STAGE_TYPES)
        val = st.selectbox("Assign stage @type", opts,
                           index=opts.index(pv) if pv in opts else 0, key=f"d_{key}")
        return val, ""

    if cat == "speaker missing id":
        opts = ["", *roles, "↳ other (type below)"]
        idx = opts.index(role_default) if role_default in opts else 0
        pick = st.selectbox("Link speaker to role xml:id", opts, index=idx, key=f"d_{key}")
        rid = pick
        if pick == "↳ other (type below)":
            rid = st.text_input("role xml:id", value=role_default, key=f"rid_{key}")
        return ("linked" if rid and rid != "↳ other (type below)" else ""), rid

    if cat == "missing cast member":
        opts = ["", "add as character", "collective (no cast entry)", "not a character / ignore"]
        val = st.radio("Disposition", opts, index=opts.index(pv) if pv in opts else 0,
                       key=f"d_{key}", horizontal=True)
        rid = ""
        if val == "add as character":
            ro = ["↳ new id (type)"] + roles
            pick = st.selectbox("canonical role id", ro, key=f"role_{key}")
            rid = st.text_input("role xml:id", value=role_default, key=f"rid_{key}") if pick.startswith("↳") else pick
        return val, rid

    if cat == "collective speaker":
        opts = ["", "confirm collective (no individual entry)", "actually an individual (add to cast)"]
        return st.radio("Confirm", opts, index=opts.index(pv) if pv in opts else 0,
                        key=f"d_{key}"), ""

    if owner == "info":
        opts = ["acknowledge (no action)", "needs action after all"]
        return st.radio("Recorded decision", opts, index=opts.index(pv) if pv in opts else 0,
                        key=f"d_{key}"), ""

    # legacy attr / legacy tag / fallback
    opts = ["", "apply suggested fix", "leave as-is", "needs discussion"]
    return st.radio("Decision", opts, index=opts.index(pv) if pv in opts else 0,
                    key=f"d_{key}", horizontal=True), ""


# ── Render ────────────────────────────────────────────────────────────────────
def render() -> None:
    st.header("📜 YiDraCor — annotation flags")
    st.caption("Resolve pipeline flags from the Lateiner-Hurwitz editions. "
               "Each decision is saved to the repo for the annotation pipeline to apply.")

    flags = load_flags(_mtime(ENRICHED))
    if not flags:
        st.warning(f"No enriched flags file found at `{ENRICHED}`. "
                   "Run `code/annotation/make_flag_crops.py` first.")
        return
    decisions = load_decisions(_mtime(DECISIONS))
    reviewer = st.session_state.get("reviewer", "?")

    # ── Filters ──────────────────────────────────────────────────────────────
    editions = sorted({r["edition"] for r in flags})
    cats = sorted({r["category"] for r in flags})
    c1, c2, c3 = st.columns([2, 2, 1])
    f_ed = c1.multiselect("Edition", editions, default=editions)
    f_cat = c2.multiselect("Category", cats, default=cats)
    only_todo = c3.toggle("Undecided only", value=False)

    rows = [r for r in flags if r["edition"] in f_ed and r["category"] in f_cat]
    if only_todo:
        rows = [r for r in rows if row_key(r) not in decisions]

    done = sum(1 for r in flags if row_key(r) in decisions)
    st.progress(done / len(flags), text=f"{done} / {len(flags)} flags decided")
    st.divider()

    for r in rows:
        key = row_key(r)
        prev = decisions.get(key, {})
        decided = key in decisions
        folder = ""
        if resolve_edition and load_editions:
            ed = resolve_edition(r["edition"], load_editions())
            folder = ed["folder"] if ed else ""
        roles = cast_roles(folder) if folder else []

        badge = "✅" if decided else "•"
        with st.container(border=True):
            st.markdown(f"**{badge} {r['edition']} · p{r['page(s)']} · `{r['line_id / count']}`** "
                        f"— _{r['category']}_  ·  owner: `{r['owner']}`")
            left, right = st.columns([3, 4])
            with left:
                cp = r.get("crop_path", "")
                img = YIDRACOR / cp if cp else None
                if img and img.exists():
                    st.image(str(img), use_container_width=True)
                else:
                    st.caption("_(no single-line crop — multi-page / section flag)_")
                if r.get("text"):
                    st.markdown(f"<div dir='rtl' style='font-size:1.4em'>{r['text']}</div>",
                                unsafe_allow_html=True)
                    st.caption(f"≈ *{r.get('translit','')}*")
            with right:
                st.markdown(f"**Issue:** {r.get('issue/detail','')}")
                st.markdown(f"**Suggested:** {r.get('suggested_action','')}")
                if r.get("transkribus_url"):
                    st.link_button("↗ Open page in Transkribus", r["transkribus_url"])
                dec_val, role_id = decision_widget(r, key, prev, roles)
                note = st.text_input("Note", value=prev.get("note", ""), key=f"n_{key}")
                if st.button("💾 Save", key=f"s_{key}", type="primary"):
                    save_decision({
                        "row_key": key, "edition": r["edition"], "page": r["page(s)"],
                        "line": r["line_id / count"], "category": r["category"],
                        "owner": r["owner"], "decision": dec_val, "role_id": role_id,
                        "note": note, "reviewer": reviewer,
                        "updated": _dt.datetime.now().isoformat(timespec="seconds"),
                    })
                    st.toast("Saved.", icon="✅")
                    st.rerun()
