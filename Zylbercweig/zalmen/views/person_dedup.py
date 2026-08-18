"""Person dedup — pair-review UI for cross-volume same-person decisions.

First batch: 208 alias-surfaced pseudonym candidates from
`Zylbercweig/people/pseudonym_candidates_review.tsv`. Future batches (full
14k dedup pairs, mention-to-subject, etc.) plug in via BATCHES below — decisions
are keyed by (a_xml_id, b_xml_id) so they persist across batches.
"""
from __future__ import annotations

import csv
import datetime as _dt
import pathlib
import sys

import streamlit as st
from atomic_io import atomic_write

BASE = pathlib.Path(__file__).parents[2]
_BASE_STR = str(BASE)
if _BASE_STR not in sys.path:
    sys.path.insert(0, _BASE_STR)

PEOPLE_DIR = BASE / "people"
DECISIONS_TSV = PEOPLE_DIR / "person_dedup_decisions.tsv"
REPO_DECISIONS_PATH = "Zylbercweig/people/person_dedup_decisions.tsv"
PEOPLE_EXTRACTED_TSV = PEOPLE_DIR / "people_extracted.tsv"

# Pluggable batch sources. Add new batches here as they come online.
BATCHES = {
    "Pseudonym candidates (208)": PEOPLE_DIR / "pseudonym_candidates_review.tsv",
    "Full dedup candidates (14k)": PEOPLE_DIR / "people_dedup_candidates.tsv",
}

DECISION_FIELDS = [
    "a_xml_id", "b_xml_id", "decision", "reviewer", "reviewed_at", "notes",
    "a_heading", "b_heading", "composite", "batch",
]


# ── decisions persistence ────────────────────────────────────────────────────
def _load_decisions() -> dict[tuple[str, str], dict]:
    if not DECISIONS_TSV.exists():
        return {}
    out: dict[tuple[str, str], dict] = {}
    with open(DECISIONS_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            a, b = r.get("a_xml_id", ""), r.get("b_xml_id", "")
            if not (a and b):
                continue
            key = tuple(sorted((a, b)))
            out[key] = r
    return out


def _save_decision(row: dict) -> None:
    decisions = _load_decisions()
    key = tuple(sorted((row["a_xml_id"], row["b_xml_id"])))
    decisions[key] = {**decisions.get(key, {}), **row}
    PEOPLE_DIR.mkdir(parents=True, exist_ok=True)
    with atomic_write(DECISIONS_TSV) as fp:
        w = csv.DictWriter(fp, fieldnames=DECISION_FIELDS, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for v in decisions.values():
            w.writerow(v)
    try:
        from zalmen.github_sync import push_file_to_github
        push_file_to_github(
            REPO_DECISIONS_PATH,
            DECISIONS_TSV,
            f"person dedup: {row['decision']} {row['a_xml_id']}↔{row['b_xml_id']}",
        )
    except Exception:
        pass  # local-only fallback


# ── pair loading ─────────────────────────────────────────────────────────────
def _load_pairs(path: pathlib.Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path) as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data
def _load_entries_by_xml_id() -> dict[str, dict]:
    """xml_id -> full subject-entry row from people_extracted.tsv."""
    if not PEOPLE_EXTRACTED_TSV.exists():
        return {}
    out: dict[str, dict] = {}
    with open(PEOPLE_EXTRACTED_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            xml = (r.get("xml_id") or "").strip()
            if xml:
                out[xml] = r
    return out


def _render_entry_details(entry: dict) -> None:
    """Render the metadata block inside the card's expander."""
    if not entry:
        st.caption("No extracted entry data for this xml_id.")
        return
    pairs = [
        ("entry_type", entry.get("entry_type")),
        ("span", entry.get("span")),
        ("birth", entry.get("birth_date")),
        ("death", entry.get("death_date")),
        ("birth_place", " · ".join(x for x in (
            entry.get("birth_place_name"),
            entry.get("birth_place_province"),
            entry.get("birth_place_country"),
        ) if x)),
        ("death_place", " · ".join(x for x in (
            entry.get("death_place_name"),
            entry.get("death_place_province"),
            entry.get("death_place_country"),
        ) if x)),
        ("burial_place", entry.get("burial_place_name")),
        ("subheading", entry.get("subheading")),
        ("education", entry.get("education")),
        ("names_variants", entry.get("names_variants")),
        ("mentions_in_entry", entry.get("people_mentions_count")),
        ("endnotes", entry.get("endnotes_count")),
    ]
    for k, v in pairs:
        v = (v or "").strip() if isinstance(v, str) else v
        if not v:
            continue
        if any("֐" <= c <= "׿" for c in str(v)):
            st.markdown(
                f"**{k}:** <span dir='rtl'>{v}</span>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(f"**{k}:** {v}")


# ── main view ────────────────────────────────────────────────────────────────
def render() -> None:
    st.header("B1 · Person Dedup")
    st.caption(
        "Decide whether two Zylbercweig entries are the same person. "
        "Decisions persist by xml_id pair, across batches."
    )

    batch_label = st.selectbox("Batch", list(BATCHES.keys()), key="pd_batch")
    pairs = _load_pairs(BATCHES[batch_label])
    if not pairs:
        st.error(f"No pairs found at {BATCHES[batch_label]}")
        return

    decisions = _load_decisions()

    # Optional filters
    f1, f2, f3 = st.columns(3)
    with f1:
        show_decided = st.checkbox("Show already-decided", value=False, key="pd_show_dec")
    with f2:
        min_composite = st.slider("Min composite", 0.0, 1.0, 0.0, 0.05, key="pd_min_c")
    with f3:
        gender_buckets = st.multiselect(
            "Gender bucket",
            options=["same gender", "mixed gender", "unknown"],
            default=["same gender", "mixed gender", "unknown"],
            key="pd_gender",
            help="'unknown' = at least one side has no gender resolved from DB.",
        )

    def _key(r: dict) -> tuple[str, str]:
        return tuple(sorted((r.get("a_xml_id", ""), r.get("b_xml_id", ""))))

    def _gender_bucket(r: dict) -> str:
        ga = (r.get("a_gender") or "").strip()
        gb = (r.get("b_gender") or "").strip()
        if not ga or not gb:
            return "unknown"
        return "same gender" if ga == gb else "mixed gender"

    def _visible(r: dict) -> bool:
        if not show_decided and _key(r) in decisions and decisions[_key(r)].get("decision"):
            return False
        if _gender_bucket(r) not in gender_buckets:
            return False
        try:
            if float(r.get("composite") or r.get("composite_score") or 0) < min_composite:
                return False
        except ValueError:
            return False
        return True

    visible = [r for r in pairs if _visible(r)]

    # Progress
    n_total = len(pairs)
    n_decided = sum(1 for r in pairs if _key(r) in decisions and decisions[_key(r)].get("decision"))
    st.progress(n_decided / max(n_total, 1), text=f"{n_decided} / {n_total} decided in this batch")

    if not visible:
        st.success("Nothing left to review in this batch with current filters.")
        return

    # Position state
    pos_key = f"pd_pos_{batch_label}"
    if pos_key not in st.session_state or st.session_state[pos_key] >= len(visible):
        st.session_state[pos_key] = 0

    nav = st.columns([1, 1, 4, 1, 1])
    if nav[0].button("⏮ first", use_container_width=True):
        st.session_state[pos_key] = 0
    if nav[1].button("← prev", use_container_width=True, disabled=(st.session_state[pos_key] == 0)):
        st.session_state[pos_key] -= 1
    nav[2].caption(f"pair {st.session_state[pos_key] + 1} of {len(visible)} visible")
    if nav[3].button("next →", use_container_width=True, disabled=(st.session_state[pos_key] >= len(visible) - 1)):
        st.session_state[pos_key] += 1
    if nav[4].button("last ⏭", use_container_width=True):
        st.session_state[pos_key] = len(visible) - 1

    r = visible[st.session_state[pos_key]]
    pair_key = _key(r)
    existing = decisions.get(pair_key, {})

    st.divider()

    # Scores
    composite = r.get("composite") or r.get("composite_score") or ""
    name_score = r.get("name_score") or ""
    same_vol = r.get("same_volume") == "1"
    badge_bits = []
    if composite:
        badge_bits.append(f"composite **{composite}**")
    if name_score:
        badge_bits.append(f"name {name_score}")
    badge_bits.append("same-volume" if same_vol else "cross-volume")
    st.markdown(" · ".join(badge_bits))

    # db_id co-status banner
    a_db, b_db = (r.get("a_db_id") or "").strip(), (r.get("b_db_id") or "").strip()
    if a_db and b_db:
        if a_db == b_db:
            st.success(f"Both already aligned to DB id **{a_db}** — already considered same person.")
        else:
            st.warning(
                f"Different DB ids ({a_db} vs {b_db}). 'Same' here implies a DB merge."
            )
    elif a_db or b_db:
        st.info(f"Only one side has a DB id ({a_db or b_db}). 'Same' will adopt it for both.")

    # A / B side-by-side
    entries = _load_entries_by_xml_id()
    left, right = st.columns(2, gap="large")
    for col, side in [(left, "a"), (right, "b")]:
        with col:
            heading = r.get(f"{side}_heading", "")
            aliases = r.get(f"{side}_aliases", "")
            vol = r.get(f"{side}_volume", "")
            xml = r.get(f"{side}_xml_id", "")
            gender = (r.get(f"{side}_gender") or "").strip() or "?"
            db_id = (r.get(f"{side}_db_id") or "").strip() or "—"
            with st.container(border=True):
                st.markdown(f"**Vol {vol}** · `{xml}` · gender: {gender} · db_id: {db_id}")
                st.markdown(
                    f"<div dir='rtl' style='font-size:1.3rem; font-weight:600'>{heading}</div>",
                    unsafe_allow_html=True,
                )
                if aliases:
                    st.caption("aliases:")
                    st.markdown(
                        f"<div dir='rtl' style='font-size:1.05rem'>{aliases}</div>",
                        unsafe_allow_html=True,
                    )
                with st.expander("entry details"):
                    _render_entry_details(entries.get(xml, {}))

    st.divider()

    # Existing decision banner
    if existing.get("decision"):
        st.info(
            f"Previously: **{existing['decision']}** "
            f"by {existing.get('reviewer', '?')} at {existing.get('reviewed_at', '?')}"
            + (f" — {existing['notes']}" if existing.get("notes") else "")
        )

    # Decision buttons
    notes = st.text_input("Notes (optional)", value=existing.get("notes", ""), key=f"pd_notes_{pair_key}")

    def _record(decision: str) -> None:
        _save_decision({
            "a_xml_id": r.get("a_xml_id", ""),
            "b_xml_id": r.get("b_xml_id", ""),
            "decision": decision,
            "reviewer": st.session_state.get("reviewer", ""),
            "reviewed_at": _dt.datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "notes": notes,
            "a_heading": r.get("a_heading", ""),
            "b_heading": r.get("b_heading", ""),
            "composite": composite,
            "batch": batch_label,
        })
        # advance
        if st.session_state[pos_key] < len(visible) - 1:
            st.session_state[pos_key] += 1
        st.rerun()

    b1, b2, b3, _ = st.columns([1, 1, 1, 2])
    if b1.button("✅ Same person", type="primary", use_container_width=True):
        _record("same")
    if b2.button("❌ Different", use_container_width=True):
        _record("different")
    if b3.button("⏭ Defer", use_container_width=True):
        _record("defer")
