"""Person Hub — person-centric browser over the Phase C entity model.

Read-only. One hub = one person, linking subject entries across volumes,
external DB rows, and validated mention surfaces. Regenerate the underlying
data with build_person_hub.py after new B1/B2 decisions land.
"""
from __future__ import annotations

import csv
import pathlib
import sys

import streamlit as st

BASE = pathlib.Path(__file__).parents[2]
PEOPLE_DIR = BASE / "people"
HUB_TSV = PEOPLE_DIR / "person_hub.tsv"
MEMBERS_TSV = PEOPLE_DIR / "person_hub_members.tsv"
RESOLUTIONS_TSV = PEOPLE_DIR / "mention_surname_resolutions.tsv"

MAX_RESULTS = 25


def _mtime(p: pathlib.Path) -> float:
    return p.stat().st_mtime if p.exists() else 0.0


@st.cache_data
def _load_hubs(mtime: float) -> list[dict]:
    with open(HUB_TSV) as f:
        return list(csv.DictReader(f, delimiter="\t"))


@st.cache_data
def _load_members(mtime: float) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    with open(MEMBERS_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            out.setdefault(r["hub_id"], []).append(r)
    return out


@st.cache_data
def _mention_counts(mtime: float) -> dict[str, int]:
    """hub_id → count of bare-surname mentions the resolver assigned to it."""
    if not RESOLUTIONS_TSV.exists():
        return {}
    csv.field_size_limit(sys.maxsize)
    out: dict[str, int] = {}
    with open(RESOLUTIONS_TSV) as f:
        for r in csv.DictReader(f, delimiter="\t"):
            hid = r.get("resolved_hub_id", "")
            if hid:
                out[hid] = out.get(hid, 0) + 1
    return out


def _rtl(text: str, size: float = 1.0, weight: int = 400) -> str:
    return (f"<div dir='rtl' style='font-size:{size}rem; "
            f"font-weight:{weight}'>{text}</div>")


def render() -> None:
    st.header("Person Hub")
    st.caption(
        "One hub = one person: subject entries across volumes + DB rows + "
        "validated mention surfaces, linked by confirmed evidence only."
    )
    if not HUB_TSV.exists():
        st.error(f"No hub data — run build_person_hub.py first ({HUB_TSV}).")
        return

    hubs = _load_hubs(_mtime(HUB_TSV))
    members = _load_members(_mtime(MEMBERS_TSV))
    mention_counts = _mention_counts(_mtime(RESOLUTIONS_TSV))

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Hubs", len(hubs))
    m2.metric("Multi-entry", sum(1 for h in hubs if int(h["n_entries"]) > 1))
    m3.metric("DB-aligned", sum(1 for h in hubs if int(h["n_db_rows"]) > 0))
    m4.metric("Pending drafts", sum(int(h["pending_drafts"]) for h in hubs))

    q = (st.text_input("Search (heading, surface, person_id, db_id, hub_id)",
                       key="ph_q") or "").strip()
    only_multi = st.checkbox("Only multi-entry hubs", value=False, key="ph_multi")

    def _match(h: dict) -> bool:
        if only_multi and int(h["n_entries"]) < 2:
            return False
        if not q:
            return True
        hay = "\t".join([h["hub_id"], h["canonical_heading"], h["entry_person_ids"],
                         h["db_ids"], h["top_surfaces"]])
        for m in members.get(h["hub_id"], []):
            hay += "\t" + m["label"] + "\t" + m["member_id"]
        return q in hay

    results = [h for h in hubs if _match(h)]
    st.caption(f"{len(results)} hubs match" + (f" — showing first {MAX_RESULTS}"
               if len(results) > MAX_RESULTS else ""))

    for h in results[:MAX_RESULTS]:
        hid = h["hub_id"]
        badge = " 🔀" if int(h["n_entries"]) > 1 else ""
        drafts = f" · 🤖 {h['pending_drafts']} pending draft(s)" if int(h["pending_drafts"]) else ""
        with st.expander(f"{hid}{badge} — {h['canonical_heading']}"):
            bits = []
            if h.get("birth_date") or h.get("death_date"):
                bits.append(f"{h.get('birth_date', '?')} – {h.get('death_date', '?')}")
            bits.append(f"evidence: {h['evidence'] or '—'}")
            n_res = mention_counts.get(hid, 0)
            if n_res:
                bits.append(f"{n_res} bare-surname mentions resolved here")
            st.caption(" · ".join(bits) + drafts)
            for kind, icon in [("entry", "📖"), ("db", "🗄"), ("surface", "💬")]:
                rows = [m for m in members.get(hid, []) if m["member_kind"] == kind]
                if not rows:
                    continue
                st.markdown(f"**{icon} {kind} ({len(rows)})**")
                for m in rows:
                    detail = f" — {m['detail']}" if m["detail"] else ""
                    ev = f" · _{m['evidence']}_" if m["evidence"] else ""
                    st.markdown(
                        f"<div dir='rtl' style='margin-right:1em'>"
                        f"<code>{m['member_id']}</code> {m['label']}{detail}{ev}</div>",
                        unsafe_allow_html=True)
