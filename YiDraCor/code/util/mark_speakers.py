"""
Parse flat <lb/>-based XML into DraCor-compatible <sp> elements.

Reads the single <div> in <body>, identifies speakers by the '|' delimiter,
and builds <sp who="#id"><speaker>...</speaker><p>...</p></sp> structure.
"""

import re
import logging
from copy import deepcopy
from lxml import etree

from util.speaker_map import SPEAKER_VARIANTS, PERSON_LIST, SCENE_MARKERS

logger = logging.getLogger(__name__)

TEI = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"

# Tags to unwrap (keep text content, remove element wrapper)
UNWRAP_TAGS = {"_underline", "_insertion", "_replacement", "_OtherHand"}
# Tags to delete entirely (remove element and its content)
DELETE_TAGS = {"_comment", "_deleted"}

# Act boundary patterns: (regex, act_number or None for end-markers)
ACT_START_PATTERNS = [
    (r"1ter\s+Act", 1),
    (r"2ter\s+Act", 2),
    (r"דריטער\s+אקט", 3),
    (r"פֿערטער\s+אקט", 4),
    (r"5טע\s+אקט", 5),
]

ACT_END_PATTERNS = [
    r"ענדע\s+ערשטען\s+אקט",
    r"ענדע\s+2טען\s+אקט",
    r"ענדע\s+פֿיערטען\s+אקט",
    r"^III\s+אקט",  # End-of-act-3 label
    r"^ענדע$",
]


def _strip_tags(text):
    """Remove all XML tags from a string."""
    return re.sub(r"<[^>]+>", "", text)


def _local(tag):
    """Get local name from potentially namespaced tag."""
    if isinstance(tag, str) and "}" in tag:
        return tag.split("}")[1]
    return str(tag) if tag else ""


def _serialize_text(el):
    """Serialize an element and all its children to plain text."""
    return etree.tostring(el, encoding="unicode", method="text") or ""


def _get_line_content(lb):
    """
    Collect content between this <lb/> and the next <lb/>/<pb/>.
    Returns (plain_text, list_of_elements_between).
    The plain_text is for speaker detection; elements are for content preservation.
    """
    parts_text = []
    elements = []

    # lb.tail is the text immediately after <lb/>
    if lb.tail:
        parts_text.append(lb.tail)

    sib = lb.getnext()
    while sib is not None:
        local = _local(sib.tag)
        if local in ("lb", "pb"):
            break
        # Get full text of this element
        parts_text.append(_serialize_text(sib))
        if sib.tail:
            parts_text.append(sib.tail)
        elements.append(sib)
        sib = sib.getnext()

    return "".join(parts_text), elements


def _clean_speaker(raw):
    """Clean raw speaker text for lookup in SPEAKER_VARIANTS."""
    # Strip XML tags if any leaked through
    cleaned = _strip_tags(raw).strip()
    return cleaned


def _is_act_start(text):
    """Check if text marks the start of an act. Returns act number or None."""
    clean = _strip_tags(text).strip()
    for pattern, act_num in ACT_START_PATTERNS:
        if re.search(pattern, clean):
            return act_num
    return None


def _is_act_end(text):
    """Check if text is an end-of-act marker."""
    clean = _strip_tags(text).strip()
    for pattern in ACT_END_PATTERNS:
        if re.search(pattern, clean):
            return True
    return False


def _build_p_content(lines, ns):
    """
    Build a <p> element from collected line content.
    Each entry in lines is a list of ("text", str) or ("elem", lxml.Element) nodes.
    Element tails are tracked as separate ("text", ...) nodes in the list,
    so we strip the tail from copied elements to avoid duplication.
    Preserves <foreign>, <stage>, <choice>, <hi> inline markup.
    Unwraps/deletes non-standard tags.
    """
    p = etree.SubElement(etree.Element("dummy"), f"{{{TEI}}}p")

    def _append_text(text):
        """Append text to last element's tail or to p.text."""
        children = list(p)
        if children:
            children[-1].tail = (children[-1].tail or "") + text
        else:
            p.text = (p.text or "") + text

    first = True
    for line_nodes in lines:
        if not first:
            lb = etree.SubElement(p, f"{{{TEI}}}lb")
        else:
            first = False

        for ntype, content in line_nodes:
            if ntype == "text":
                _append_text(content)
            elif ntype == "elem":
                el_copy = deepcopy(content)
                # Strip tail — it's tracked as a separate text node in the list
                el_copy.tail = None
                p.append(el_copy)

    # Clean non-standard tags after building (needs parent relationships)
    _clean_element(p)
    return p


def _clean_element(el):
    """Recursively clean an element: unwrap/delete non-standard tags."""
    # Process children first (iterate over a copy of the list)
    for child in list(el):
        _clean_element(child)

    local = _local(el.tag)

    if local in DELETE_TAGS:
        # Remove element entirely, preserve tail
        parent = el.getparent()
        if parent is not None:
            prev = el.getprevious()
            if el.tail:
                if prev is not None:
                    prev.tail = (prev.tail or "") + el.tail
                else:
                    parent.text = (parent.text or "") + el.tail
            parent.remove(el)

    elif local in UNWRAP_TAGS:
        # Keep text and children, remove the wrapper
        parent = el.getparent()
        if parent is not None:
            idx = list(parent).index(el)
            prev = el.getprevious()

            # Move element's text
            if el.text:
                if prev is not None:
                    prev.tail = (prev.tail or "") + el.text
                else:
                    parent.text = (parent.text or "") + el.text

            # Move children
            for i, child in enumerate(list(el)):
                parent.insert(idx + i, child)

            # Move tail
            if el.tail:
                children = list(el)
                if children:
                    last_child = children[-1]
                    last_child.tail = (last_child.tail or "") + el.tail
                elif prev is not None:
                    prev.tail = (prev.tail or "") + el.tail
                else:
                    parent.text = (parent.text or "") + el.tail

            parent.remove(el)


def _collect_line_nodes(lb):
    """
    Collect content between this <lb/> and the next <lb/>/<pb/> as structured nodes.
    Returns list of ("text", str) or ("elem", lxml.Element).
    """
    nodes = []
    if lb.tail:
        nodes.append(("text", lb.tail))
    sib = lb.getnext()
    while sib is not None:
        local = _local(sib.tag)
        if local in ("lb", "pb"):
            break
        nodes.append(("elem", sib))
        if sib.tail:
            nodes.append(("text", sib.tail))
        sib = sib.getnext()
    return nodes


def _nodes_to_plain(nodes):
    """Convert node list to plain text for speaker detection."""
    parts = []
    for ntype, content in nodes:
        if ntype == "text":
            parts.append(content)
        else:
            parts.append(_serialize_text(content))
    return "".join(parts)


def _split_at_pipe(lb):
    """
    Split an <lb/> line at the first '|' character.
    Returns (speaker_raw, after_nodes) or None if no pipe.
    after_nodes is a list of ("text", str) or ("elem", element) for the content after pipe.
    """
    nodes = _collect_line_nodes(lb)
    plain = _nodes_to_plain(nodes)

    if "|" not in plain:
        return None

    # Find where the pipe is in the node sequence
    before_text_parts = []
    after_nodes = []
    found_pipe = False

    for ntype, content in nodes:
        if found_pipe:
            after_nodes.append((ntype, content))
            continue

        if ntype == "text" and "|" in content:
            before_part, after_part = content.split("|", 1)
            before_text_parts.append(before_part)
            if after_part:
                after_nodes.append(("text", after_part))
            found_pipe = True
        elif ntype == "elem":
            el_text = _serialize_text(content)
            if "|" in el_text:
                # Pipe is inside an element — treat the whole element as speaker context
                before_text_parts.append(el_text.split("|", 1)[0])
                after_part = el_text.split("|", 1)[1]
                if after_part:
                    after_nodes.append(("text", after_part))
                found_pipe = True
            else:
                before_text_parts.append(el_text)
        else:
            before_text_parts.append(content)

    if not found_pipe:
        return None

    speaker_raw = _clean_speaker("".join(before_text_parts))
    return speaker_raw, after_nodes


def parse_body(doc):
    """
    Parse the flat <div> body into structured segments.

    Returns a list of dicts, each being one of:
    - {"type": "act_start", "n": int, "text": str}
    - {"type": "act_end", "text": str}
    - {"type": "sp", "who": str, "speaker": str, "lines": [...], "warnings": [...]}
    - {"type": "stage", "element": lxml.Element}
    - {"type": "pb", "element": lxml.Element}
    - {"type": "skip"} for dramatis personae, markers, etc.
    """
    nsmap = {"tei": TEI}
    body_div = doc.tree.xpath("//tei:body/tei:div", namespaces=nsmap)
    if not body_div:
        raise ValueError("No <div> found in <body>")
    div = body_div[0]

    segments = []
    current_sp = None
    in_preamble = True  # Before first act
    seen_acts = set()  # Track which act numbers we've already opened

    def flush_sp():
        nonlocal current_sp
        if current_sp and current_sp["lines"]:
            segments.append(current_sp)
        current_sp = None

    def try_act_start(text):
        """Check for act start, skip duplicates. Returns True if handled."""
        nonlocal in_preamble
        act_n = _is_act_start(text)
        if act_n and act_n not in seen_acts:
            flush_sp()
            seen_acts.add(act_n)
            segments.append({"type": "act_start", "n": act_n, "text": text.strip()})
            in_preamble = False
            return True
        if act_n and act_n in seen_acts:
            return True  # Skip duplicate
        return False

    def try_act_end(text):
        """Check for act end marker. Returns True if handled."""
        if _is_act_end(text):
            flush_sp()
            segments.append({"type": "act_end", "text": text.strip()})
            return True
        return False

    for child in div:
        local = _local(child.tag)

        if local == "pb":
            # Page break - preserve as milestone
            segments.append({"type": "pb", "element": deepcopy(child)})
            continue

        if local == "stage":
            # Standalone stage direction
            flush_sp()
            segments.append({"type": "stage", "element": deepcopy(child)})
            continue

        if local != "lb":
            # Skip non-lb elements (hi, _underline used as block wrappers, etc.)
            # But check if they contain text that might be act markers
            text = _serialize_text(child)
            if try_act_start(text):
                continue
            if try_act_end(text):
                continue
            continue

        # It's an <lb/> element
        line_text, line_elems = _get_line_content(child)
        plain = line_text.strip()

        if not plain:
            continue

        # Check for act markers
        if try_act_start(plain):
            continue
        if try_act_end(plain):
            continue

        if in_preamble:
            continue  # Skip dramatis personae

        # Try to split at pipe
        pipe_result = _split_at_pipe(child)

        if pipe_result is not None:
            speaker_raw, after_nodes = pipe_result

            # Check if it's a scene marker, not a speaker
            if speaker_raw in SCENE_MARKERS:
                continue

            # Empty speaker = standalone stage direction with pipe
            if not speaker_raw:
                flush_sp()
                continue

            # Look up speaker
            speaker_id = SPEAKER_VARIANTS.get(speaker_raw)
            if speaker_id is None:
                # Likely a false-positive pipe (mid-dialogue pipe)
                line_nodes = _collect_line_nodes(child)
                if current_sp:
                    current_sp["lines"].append(line_nodes)
                    logger.debug(f"False pipe, appending to current speech: [{speaker_raw}]")
                else:
                    logger.warning(f"Unknown speaker and no current speech: [{speaker_raw}]")
                continue

            # Valid speaker found - start new speech
            flush_sp()
            current_sp = {
                "type": "sp",
                "who": speaker_id,
                "speaker": PERSON_LIST.get(speaker_id, speaker_raw),
                "lines": [after_nodes],
                "warnings": [],
            }
        else:
            # No pipe - continuation of current speech
            if current_sp:
                line_nodes = _collect_line_nodes(child)
                current_sp["lines"].append(line_nodes)
            # else: orphan line (between acts, stage direction text, etc.)

    flush_sp()
    return segments


def build_dracor_tree(segments, project):
    """
    Build a complete DraCor-compatible TEI XML tree from parsed segments.
    """
    # Collect which characters actually appear
    used_ids = set()
    for seg in segments:
        if seg["type"] == "sp":
            used_ids.add(seg["who"])

    # Build root
    root = etree.Element(f"{{{TEI}}}TEI", nsmap={None: TEI})

    # -- teiHeader --
    header = etree.SubElement(root, f"{{{TEI}}}teiHeader")

    file_desc = etree.SubElement(header, f"{{{TEI}}}fileDesc")
    title_stmt = etree.SubElement(file_desc, f"{{{TEI}}}titleStmt")
    title = etree.SubElement(title_stmt, f"{{{TEI}}}title")
    title.text = project

    pub_stmt = etree.SubElement(file_desc, f"{{{TEI}}}publicationStmt")
    p_pub = etree.SubElement(pub_stmt, f"{{{TEI}}}p")

    source_desc = etree.SubElement(file_desc, f"{{{TEI}}}sourceDesc")
    bibl = etree.SubElement(source_desc, f"{{{TEI}}}bibl")
    bibl_title = etree.SubElement(bibl, f"{{{TEI}}}title")
    bibl_title.text = project

    # profileDesc with particDesc
    profile_desc = etree.SubElement(header, f"{{{TEI}}}profileDesc")
    partic_desc = etree.SubElement(profile_desc, f"{{{TEI}}}particDesc")
    list_person = etree.SubElement(partic_desc, f"{{{TEI}}}listPerson")

    for xml_id, display_name in PERSON_LIST.items():
        if xml_id in used_ids:
            person = etree.SubElement(list_person, f"{{{TEI}}}person")
            person.set(f"{{{XML_NS}}}id", xml_id)
            name = etree.SubElement(person, f"{{{TEI}}}persName")
            name.text = display_name

    # -- text/body --
    text_el = etree.SubElement(root, f"{{{TEI}}}text")
    body = etree.SubElement(text_el, f"{{{TEI}}}body")

    # Group segments into acts
    current_act_div = None
    current_scene_div = None
    act_count = 0
    scene_count = 0

    for seg in segments:
        if seg["type"] == "act_start":
            # Close previous
            act_count += 1
            scene_count = 1
            current_act_div = etree.SubElement(body, f"{{{TEI}}}div")
            current_act_div.set("type", "act")
            current_act_div.set("n", str(seg["n"]))
            head = etree.SubElement(current_act_div, f"{{{TEI}}}head")
            head.text = seg["text"]
            # Start first scene
            current_scene_div = etree.SubElement(current_act_div, f"{{{TEI}}}div")
            current_scene_div.set("type", "scene")
            current_scene_div.set("n", str(scene_count))
            continue

        if seg["type"] == "act_end":
            current_act_div = None
            current_scene_div = None
            continue

        # Skip content between acts (pb, stage between act_end and act_start)
        if current_act_div is None:
            continue

        # Ensure we have a scene container within the act
        if current_scene_div is None:
            scene_count = 1
            current_scene_div = etree.SubElement(current_act_div, f"{{{TEI}}}div")
            current_scene_div.set("type", "scene")
            current_scene_div.set("n", str(scene_count))

        target = current_scene_div

        if seg["type"] == "pb":
            target.append(seg["element"])

        elif seg["type"] == "stage":
            el = seg["element"]
            _clean_element(el)
            target.append(el)

        elif seg["type"] == "sp":
            sp = etree.SubElement(target, f"{{{TEI}}}sp")
            sp.set("who", f"#{seg['who']}")
            speaker = etree.SubElement(sp, f"{{{TEI}}}speaker")
            speaker.text = seg["speaker"]
            p = _build_p_content(seg["lines"], TEI)
            sp.append(p)

    return etree.ElementTree(root)
