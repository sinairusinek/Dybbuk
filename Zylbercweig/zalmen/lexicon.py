"""Shared Lexicon XML access for the org/person review views.

Historically each view (org_review, org_alignment, org_addresses, org_clusters)
carried its own `_load_all_xml` that eagerly parsed all seven
Structured_Volume*.xml files (~47 MB on disk → ~400 MB of ElementTree objects)
and pinned them for the process lifetime via `@st.cache_resource`. With four
independent copies, a session that touched several views could hold well over a
gigabyte of duplicated trees — enough to OOM the Streamlit Cloud instance. When
a volume then failed to parse under memory pressure it was silently dropped, and
every entry in it rendered as "Entry not found in XML" for the rest of the app's
life (the `None` result was cached too). That is the "some texts don't load" bug.

This module replaces that with a single, shared, lazy, per-volume loader:
  - Parses one volume on demand (only when an entry from it is actually opened).
  - Keeps only a compact `{xml_id: text}` index — plain strings, ~10x smaller
    than the tree — and lets the parsed tree be freed immediately after.
  - Catches *any* parse failure (not just ET.ParseError) and returns an empty
    index rather than propagating, so one bad file can't take down a lookup.

Because the cache key is the volume name, at most seven small string indices are
ever resident, shared across all views — instead of four full tree sets.
"""
from __future__ import annotations

import pathlib
import re
import xml.etree.ElementTree as ET

import streamlit as st

# zalmen/lexicon.py -> parents[1] == Zylbercweig/
LEXICON_DIR = pathlib.Path(__file__).parents[1] / "The Lexicon"

# Source JSON name (as stored in the `File` column of the mention data) → the
# Structured TEI file that holds the full entry <div>s. Kept here as the single
# source of truth; the views import this for their "Entry not found (…)" caption.
JSON_TO_XML: dict[str, str] = {
    "Volume5IIIorg.json": "Structured_Volume5III.xml",
    "Volume_3IIIorg.json": "Structured_Volume_3III.xml",
    "Volume_4IIIorg.json": "Structured_Volume_4III.xml",
    "volume6IIIorg.json": "Structured_volume6III.xml",
    "volume7IIIorg.json": "Structured_volume7III.xml",
    "volume_1IIIorg.json": "Structured_volume_1III.xml",
    "volume_2IIIorg.json": "Structured_volume_2III.xml",
}

TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_ID = "{http://www.w3.org/XML/1998/namespace}id"


@st.cache_resource(show_spinner=False)
def _entry_index(xml_name: str) -> dict[str, str]:
    """Parse ONE volume and return {xml_id: entry_text}.

    Cached per volume so only the files actually opened are parsed, the parsed
    tree is discarded right after indexing (only the small string dict stays
    resident), and a parse failure degrades to an empty index instead of
    crashing or being pinned as a permanent None.
    """
    path = LEXICON_DIR / xml_name
    if not path.exists():
        return {}
    try:
        tree = ET.parse(path)
    except Exception:  # noqa: BLE001 — a malformed/oversized file must not brick lookups
        return {}
    idx: dict[str, str] = {}
    for el in tree.getroot().iter(f"{{{TEI_NS}}}div"):
        xid = el.get(XML_ID)
        if not xid:
            continue
        # A single recursive itertext() over the entry <div>. The previous form
        # (join over el.iter(), calling itertext() on each descendant) emitted
        # every text node once per ancestor — the div's own itertext() already
        # covered the whole subtree, so each child re-emitted its portion and
        # the entry text came out doubled.
        text = re.sub(r"\s+", " ", " ".join(el.itertext())).strip()
        if text:
            idx[xid] = text
    return idx


def get_entry_text(json_file: str, xml_id: str) -> str | None:
    """Full entry text for a mention, by its source file + entry xml:id.

    `json_file` is the `File` value from the mention data (a JSON name); it is
    mapped to the Structured TEI file. An `.xml` name is also accepted directly.
    Returns None when the file/id is unknown or the entry has no text.
    """
    if not json_file or not xml_id:
        return None
    xml_name = JSON_TO_XML.get(json_file)
    if xml_name is None:
        xml_name = json_file if json_file.endswith(".xml") else None
    if not xml_name:
        return None
    return _entry_index(xml_name).get(xml_id) or None
