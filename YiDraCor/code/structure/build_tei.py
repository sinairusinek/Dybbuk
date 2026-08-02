"""
Structurer: assemble annotated Transkribus PAGE-XML pages into one TEI document
that validates against tei_all and renders in the YiDraCor TEI-Publisher app.

This is the missing final stage of the YiDraCor pipeline. Upstream stages
(Transkribus pull -> vocalize -> annotate) leave one PAGE-XML file per page in
`data/<play>/page_annotated/`, each TextLine carrying inline `custom` spans:

    speaker {offset; length; xmlid:<role>}   -> opens a <sp who="#role">
    stage   {offset; length; type:<t>}       -> <stage type="t">
    heading {offset; length; type:act; n:N [; subtype:songGroup]}
    lg {n:N [; cont:yes]}  l {lg_id:N}  head {lg_id:N}   -> song verse
    fw {offset; length; type:pageNum}        -> <fw> (printed page number)
    -- editorial spans (sic/corr, orig/reg, abbr/expan, supplied, ...) are
       passed through verbatim if a future RA layer adds them. Not generated.

Convention decisions baked in (see memory `diseder_two_part_tei`,
`annotation-pipeline`, and the 2026-05-24 review against A-Earliest1898.xml):
  * tei_all.rng (matches the existing TEI-Publisher corpus), not dracor schema.rng.
  * Two-part model: play acts in <body>, song supplement in <back> linked by
    @corresp; songs are NOT duplicated inline.
  * castList in <front> + listPerson in <particDesc>, both from cast_dict.json.
    Individual roles -> <person><persName>; collective/chorus roles
    (`"collective": true`) -> <personGrp><name> (DraCor convention; every
    collective speaker is a personGrp, and both take xml:id so @who resolves).
  * xml:id naming mirrors A-Earliest1898: {PlayId}_Act{n}, {PlayId}_SP{0001}_{a}.
  * <speaker> holds the printed label slice; the canonical role is on @who.
    @who takes one pointer for a solo turn and space-separated pointers for a
    joint/duet turn: `who="#a #b"` (each xmlid split and #-prefixed separately).
  * Song-supplement voice attributions ("קאָהר:", "סאלא אלט:", "סאפראן:") are
    speaker labels, not stage directions: they open an <sp><speaker> in <back>
    with @who resolved to the named singer where identifiable (the printed
    rubric stays in <speaker>), falling back to an abstract voice personGrp
    only when the voice is genuinely unattributable.

Usage:
    python3.11 -m structure.build_tei --play Di_seyder_nakht_Emkroyt_1908
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from lxml import etree

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from annotation.schema import parse_custom, dedup_entries  # canonical custom parser + de-dup

REPO_ROOT = Path(__file__).resolve().parents[2]
TEI = "http://www.tei-c.org/ns/1.0"
XML = "http://www.w3.org/XML/1998/namespace"
PAGE = "http://schema.primaresearch.org/PAGE/gts/pagecontent/2013-07-15"
NSMAP = {None: TEI}
PNS = {"p": PAGE}

# Editorial choice/critical-apparatus tags we pass through if a future layer
# adds them as inline spans. Structural tags handled explicitly are excluded.
EDITORIAL_TAGS = {
    "sic", "corr", "orig", "reg", "abbr", "expan",
    "supplied", "add", "del", "gap", "unclear", "note", "foreign", "hi",
}

# Per-play configuration. Add a block to onboard another edition.
# body_last_page: pages after it go to <back> as the song supplement — only
# Di Seder has one (apply_collective_speakers.SONG_SUPPLEMENT_FROM, verified
# 2026-07-03 that all other plays' late pages are body). NO_BACK = body-only.
NO_BACK = 10_000
CONFIG = {
    "Di_seyder_nakht_Emkroyt_1908": {
        "play_id": "DiSeder",
        "out": "tei/Di-Seder-Nakht.xml",
        "body_last_page": 54,   # play acts pp.5-54; song supplement pp.55+
        "songs_head": "געזאַנגס־טעקסט",
    },
    "AlNaharotBavel-Amkreut&Freund1909": {
        "play_id": "AlNaharot", "out": "tei/Al-Naharot-Bavel.xml",
        "body_last_page": NO_BACK},
    "BasSheva": {
        "play_id": "BasSheva", "out": "tei/Bas-Sheva.xml",
        "body_last_page": NO_BACK},
    "Blimele-AhronFaust1903": {
        "play_id": "Blimele", "out": "tei/Blimele.xml",
        "body_last_page": NO_BACK},
    "DerManUnterTiff": {
        "play_id": "DerMan", "out": "tei/Der-Man-Untern-Tisch.xml",
        "body_last_page": NO_BACK},
    "DosYudisheHerts-1910": {
        "play_id": "YudisheHerts", "out": "tei/Dos-Yudishe-Herts.xml",
        "body_last_page": NO_BACK},
    "DovidsFidele-1904": {
        "play_id": "DovidsFidele", "out": "tei/Dovids-Fidele.xml",
        "body_last_page": NO_BACK,
        # p71 = the publisher's catalogue ad (Noa 06-28: promotional,
        # non-theatrical); its stray spans must not emit post-trailer content
        "skip_pages": {71}},
    "Ezra-Emkroyt1908": {
        "play_id": "Ezra", "out": "tei/Ezra.xml",
        "body_last_page": NO_BACK},
    "HinkePinke": {
        "play_id": "HinkePinke", "out": "tei/Hinke-Pinke.xml",
        "body_last_page": NO_BACK},
    "IshahRaah": {
        "play_id": "IshaRaa", "out": "tei/Isha-Raa.xml",
        "body_last_page": NO_BACK},
    "KidushHashem": {
        "play_id": "KidushHashem", "out": "tei/Kidush-Hashem.xml",
        "body_last_page": NO_BACK},
    "MishkeMashke-Kultur1910": {
        "play_id": "MishkeMashke", "out": "tei/Mishke-Mashke.xml",
        "body_last_page": NO_BACK},
    "SoreSheyndel": {
        "play_id": "SoreSheyndel", "out": "tei/Sore-Sheyndel.xml",
        "body_last_page": NO_BACK},
    "Yudale_der_blinder,_Emkroyt1908": {
        "play_id": "Yudale", "out": "tei/Yudale-der-Blinder.xml",
        "body_last_page": NO_BACK},
    "דאס_יידישע_קינד_Dos_yudishe_kind_a_komishe_operete": {
        "play_id": "YudisheKind", "out": "tei/Dos-Yudishe-Kind.xml",
        "body_last_page": NO_BACK},
}

PAGENUM_RE = re.compile(r"^[\s—–\-—–]*\d+[\s—–\-—–]*$")


def q(tag: str) -> str:
    return f"{{{TEI}}}{tag}"


def set_xmlid(el, value: str) -> None:
    el.set(f"{{{XML}}}id", value)


# --------------------------------------------------------------------------- #
#  Loading PAGE-XML
# --------------------------------------------------------------------------- #

def load_pages(play_dir: Path):
    """Return [(page_nr, image_filename, [line_dicts])] in reading order."""
    pages = []
    for f in sorted((play_dir / "page_annotated").glob("*.xml")):
        tree = etree.parse(str(f))
        md = tree.find(".//p:TranskribusMetadata", PNS)
        page_nr = int(md.get("pageNr")) if md is not None and md.get("pageNr") else 0
        page_el = tree.find(".//p:Page", PNS)
        img = page_el.get("imageFilename") if page_el is not None else f.stem
        lines = []
        for tl in tree.findall(".//p:TextLine", PNS):
            text = tl.findtext(".//p:Unicode", namespaces=PNS) or ""
            # De-dup spans that accumulate when the pipeline re-pushes on top of
            # a prior push (Transkribus layers spans rather than replacing them);
            # otherwise duplicate heading/speaker spans emit duplicate <div>/<sp>
            # and clashing xml:ids. Same tool auto_resolve_flags uses before push.
            entries = dedup_entries(parse_custom(tl.get("custom") or ""))
            spans = [(t, a) for (t, a) in entries if t != "readingOrder"]
            lines.append({"text": text, "spans": spans})
        pages.append((page_nr, img, lines))
    return pages


def span_int(attrs: dict, key: str, default=None):
    v = attrs.get(key)
    return int(v) if v is not None and str(v).strip().lstrip("-").isdigit() else default


# --------------------------------------------------------------------------- #
#  <p> / mixed-content builder
# --------------------------------------------------------------------------- #

class Para:
    """Accumulates text, <lb/>, and inline <stage>/editorial elements into a
    target element, handling lxml's text/tail model."""

    def __init__(self, target):
        self.t = target
        self._last = None  # last child element, for tail appending

    def add_text(self, s: str):
        if not s:
            return
        if self._last is None:
            self.t.text = (self.t.text or "") + s
        else:
            self._last.tail = (self._last.tail or "") + s

    def add_child(self, el):
        self.t.append(el)
        self._last = el

    def lb(self):
        self.add_child(etree.SubElement(self.t, q("lb")))


def emit_line_content(para: Para, text: str, spans, skip_speaker=True,
                      role_ids=None, bad=None):
    """Render one source line's text into `para`, materialising inline stage
    and editorial spans, with everything else as plain text."""
    # Collect non-speaker, offset-bearing spans, sorted by offset.
    inline = []
    for tag, attrs in spans:
        if tag == "speaker" and skip_speaker:
            continue
        off = span_int(attrs, "offset")
        length = span_int(attrs, "length")
        if off is None or length is None:
            continue
        if tag == "stage" or tag in EDITORIAL_TAGS:
            inline.append((off, length, tag, attrs))
    inline.sort(key=lambda x: x[0])

    cursor = 0
    for off, length, tag, attrs in inline:
        if off < cursor:           # overlapping/nested spans: skip the inner one
            continue
        para.add_text(text[cursor:off])
        chunk = text[off:off + length]
        el = etree.Element(q(tag))
        if tag == "stage":
            stype = attrs.get("type")
            if stype:
                el.set("type", stype)
            xid = attrs.get("xmlid")
            if xid:
                el.set("who", format_who(xid, role_ids or set(),
                                         bad if bad is not None else []))
        el.text = chunk
        para.add_child(el)
        cursor = off + length
    para.add_text(text[cursor:])


def format_who(xmlid: str, role_ids: set, bad: list) -> str:
    """Build a TEI @who value from a speaker span's xmlid.

    A solo turn is a single xmlid; a joint/duet turn is space-separated xmlids
    (Noa 2026-06-14). Each token is validated against the declared roles and
    #-prefixed independently, so `"a b"` becomes `who="#a #b"` — NOT `"#a b"`.
    Unknown tokens are recorded in `bad`.
    """
    toks = xmlid.split()
    for tok in toks:
        if tok not in role_ids:
            bad.append(tok)
    return " ".join(f"#{t}" for t in toks)


def new_lg(parent, lg_attrs, pending=None):
    """Create an <lg> stanza under `parent`, carrying n / continuation.
    `pending` is a stashed song-head text that becomes the lg's <head>."""
    lg = etree.SubElement(parent, q("lg"))
    if lg_attrs is not None:
        if lg_attrs.get("n"):
            lg.set("n", lg_attrs["n"])
        if lg_attrs.get("cont") == "yes":
            lg.set("prev", "true")
    if pending:
        etree.SubElement(lg, q("head")).text = pending
    return lg


def speaker_slice(text: str, attrs: dict):
    """Return (label, rest) splitting off the printed speaker label."""
    length = span_int(attrs, "length", 0)
    label = text[:length]
    rest = text[length:]
    rest = re.sub(r"^\s*[:־\-–—]?\s*", "", rest)  # strip ': ' / maqaf
    return label.strip(), rest


# --------------------------------------------------------------------------- #
#  teiHeader / front
# --------------------------------------------------------------------------- #

def find_edition(editions_json: dict, folder: str) -> dict:
    for r in editions_json.get("editions", editions_json.values()):
        if isinstance(r, dict) and r.get("folder") == folder:
            return r
    raise SystemExit(f"No editions.json record for folder {folder}")


def build_header(rec: dict, cast: dict, play_id: str):
    header = etree.Element(q("teiHeader"))
    file_desc = etree.SubElement(header, q("fileDesc"))

    title_stmt = etree.SubElement(file_desc, q("titleStmt"))
    yid = rec.get("catalogue_yiddish_name") or rec.get("title")
    t_main = etree.SubElement(title_stmt, q("title")); t_main.set("type", "main")
    t_main.text = yid
    if rec.get("title"):
        t_sub = etree.SubElement(title_stmt, q("title")); t_sub.set("type", "sub")
        t_sub.text = rec["title"]
    if rec.get("author"):
        etree.SubElement(title_stmt, q("author")).text = rec["author"]

    pub = etree.SubElement(file_desc, q("publicationStmt"))
    etree.SubElement(pub, q("publisher")).text = "YiDraCor"
    etree.SubElement(pub, q("pubPlace")).text = "Jerusalem"
    etree.SubElement(pub, q("date")).text = "2026"

    src = etree.SubElement(file_desc, q("sourceDesc"))
    bibl = etree.SubElement(src, q("bibl"))
    etree.SubElement(bibl, q("title")).text = yid
    if rec.get("author"):
        etree.SubElement(bibl, q("author")).text = rec["author"]
    if rec.get("publisher"):
        etree.SubElement(bibl, q("publisher")).text = rec["publisher"]
    if rec.get("publication_place"):
        etree.SubElement(bibl, q("pubPlace")).text = rec["publication_place"]
    if rec.get("year_printed"):
        etree.SubElement(bibl, q("date")).text = str(rec["year_printed"])
    if rec.get("library"):
        # <repository> is msDesc-only; inside <bibl> the holding library is a
        # typed note (tei_all violation found on first corpus build 2026-08-02)
        rp = etree.SubElement(bibl, q("note")); rp.set("type", "repository")
        rp.text = rec["library"]
        if rec.get("library_signature"):
            etree.SubElement(bibl, q("idno")).text = rec["library_signature"]
    if rec.get("transkribus_url"):
        idno = etree.SubElement(bibl, q("idno")); idno.set("type", "transkribus")
        idno.text = rec["transkribus_url"]

    # particDesc / listPerson from cast_dict
    prof = etree.SubElement(header, q("profileDesc"))
    partic = etree.SubElement(prof, q("particDesc"))
    list_person = etree.SubElement(partic, q("listPerson"))
    for xmlid, info in cast.get("roles", {}).items():
        name_text = info.get("form") or info.get("bare", "")
        if info.get("collective"):
            # Collective/chorus speakers are <personGrp> in DraCor corpora, so
            # network-metric tooling treats them as group nodes. Same xml:id so
            # @who="#kor" still resolves.
            grp = etree.SubElement(list_person, q("personGrp"))
            set_xmlid(grp, xmlid)
            etree.SubElement(grp, q("name")).text = name_text
        else:
            person = etree.SubElement(list_person, q("person"))
            set_xmlid(person, xmlid)
            etree.SubElement(person, q("persName")).text = name_text
    return header, set(cast.get("roles", {}).keys())


def build_castlist(cast: dict):
    front = etree.Element(q("front"))
    cl = etree.SubElement(front, q("castList"))
    etree.SubElement(cl, q("head")).text = "פּערזאָנען"
    for xmlid, info in cast.get("roles", {}).items():
        if info.get("collective") or info.get("printed") is False:
            continue  # collective/chorus roles resolve @who via listPerson but
                      # are not printed in the dramatis personae (PI 2026-05-24).
                      # `printed: false` does the same for an abstract SOLO voice
                      # (Alt/Sopran/Bas/Tenor), which §G.4 requires be a <person>
                      # rather than a <personGrp> — so it can't use `collective`
                      # to opt out of the printed castList.
        ci = etree.SubElement(cl, q("castItem"))
        role = etree.SubElement(ci, q("role")); role.set("corresp", f"#{xmlid}")
        role.text = info.get("form") or info.get("bare", "")
    return front


# --------------------------------------------------------------------------- #
#  Body / back assembly
# --------------------------------------------------------------------------- #

def build_text(pages, cfg, role_ids):
    play_id = cfg["play_id"]
    text_el = etree.Element(q("text"))
    set_xmlid(text_el, play_id)
    text_el.set("type", play_id)

    body = etree.SubElement(text_el, q("body"))
    back = None

    state = {
        "act_div": None,
        "sp": None,
        "para": None,
        "sp_counter": 0,
        "container": body,        # where divless content/stage attaches
        "bad_who": [],
        "dropped": [],          # orphan body lines not attached to any sp/stage/div
        "in_back": False,
        "songs_div": None,
        "actsongs_div": None,
        "lg": None,
        "lg_para": None,
        "back_sp": None,    # open <sp> inside the song supplement, if any
        "back_lg": None,    # its <lg> container for continuation verse lines
        "body_lg": None,      # open <lg> for an inline body song
        "trailer_el": None,   # last emitted <trailer> in the open div
        "pending_head": None,  # song head text awaiting its <lg>
    }

    def close_sp():
        state["sp"] = None
        state["para"] = None
        state["body_lg"] = None

    def flush_pending_head():
        """A stashed song-head with no song following it is a standalone
        musical cue ((II. Ritt. Nr. 13)) -> <stage type="delivery">, matching
        the supplement path's treatment of bare rubrics."""
        if state["pending_head"] is None:
            return
        target = state["sp"] if state["sp"] is not None else state["container"]
        tag = "label" if state["in_back"] else "stage"
        st = etree.SubElement(target, q(tag))
        if tag == "stage":
            st.set("type", "delivery")
        st.text = state["pending_head"]
        state["pending_head"] = None

    def open_act(n, head_text, songgroup=False):
        nonlocal back
        flush_pending_head()
        if songgroup:
            # song-supplement act label -> an actSongs div in <back>, @corresp
            div = etree.SubElement(state["songs_div"], q("div"))
            div.set("type", "actSongs"); div.set("n", str(n))
            div.set("corresp", f"#{play_id}_Act{n}")
            etree.SubElement(div, q("head")).text = head_text
            state["actsongs_div"] = div
            state["container"] = div
            state["lg"] = None
        else:
            close_sp()
            state["trailer_el"] = None
            div = etree.SubElement(body, q("div"))
            div.set("type", "act"); div.set("n", str(n))
            set_xmlid(div, f"{play_id}_Act{n}")
            etree.SubElement(div, q("head")).text = head_text
            state["act_div"] = div
            state["container"] = div

    def open_epilog(head_text):
        # epilogue division parallel to acts (PI review 2026-05-24).
        close_sp()
        div = etree.SubElement(body, q("div"))
        div.set("type", "epilog")
        set_xmlid(div, f"{play_id}_Epilog")
        etree.SubElement(div, q("head")).text = head_text
        state["act_div"] = div
        state["container"] = div

    def enter_back():
        nonlocal back
        flush_pending_head()
        if back is None:
            back = etree.SubElement(text_el, q("back"))
            sd = etree.SubElement(back, q("div")); sd.set("type", "songs")
            etree.SubElement(sd, q("head")).text = cfg.get("songs_head", "")
            state["songs_div"] = sd
            state["container"] = sd
        close_sp()
        state["in_back"] = True
        state["act_div"] = None

    for page_nr, img, lines in pages:
        if page_nr in cfg.get("skip_pages", ()):
            continue
        if page_nr and page_nr > cfg["body_last_page"] and not state["in_back"]:
            enter_back()

        target = state["container"]
        pb = etree.SubElement(target, q("pb"))
        if page_nr:
            pb.set("n", str(page_nr))
        pb.set("facs", img)

        for line in lines:
            text, spans = line["text"], line["spans"]
            tags = {t for t, _ in spans}
            stripped = text.strip()

            # printed page number -> <fw>
            if "fw" in tags or PAGENUM_RE.match(stripped):
                fw = etree.SubElement(state["container"], q("fw"))
                fw.set("type", "pageNum")
                fw.text = stripped
                continue

            # ---- headings (act / songGroup) ----
            heading = next((a for t, a in spans if t == "heading"), None)
            if heading is not None:
                if heading.get("type") == "epilog":
                    open_epilog(stripped)
                    continue
                n = span_int(heading, "n", 1)
                if heading.get("subtype") == "songGroup":
                    if not state["in_back"]:
                        enter_back()
                    open_act(n, stripped, songgroup=True)
                else:
                    open_act(n, stripped, songgroup=False)
                continue

            # ---- song verse (lg / l / head) ----
            if not state["in_back"]:
                pass  # body path below
            if state["in_back"]:
                lg_attrs = next((a for t, a in spans if t == "lg"), None)
                speaker = next((a for t, a in spans if t == "speaker"), None)
                is_head = any(t == "head" for t, _ in spans)
                is_l = any(t == "l" for t, _ in spans)
                container = state["actsongs_div"]
                if container is None:
                    container = state["songs_div"]

                # song-supplement speaker attribution -> <sp><speaker>, NOT
                # <stage>. @who resolves to the named singer carried on the
                # span (Di Seder: karl_rizvan/rashel/kor); an untagged voice
                # label yields an <sp> with no @who that the linter surfaces
                # for an RA to resolve to a named role (or an abstract voice
                # personGrp). The sung text goes into an <lg> inside the <sp>.
                if speaker is not None:
                    state["sp_counter"] += 1
                    sp = etree.SubElement(container, q("sp"))
                    xmlid = speaker.get("xmlid", "")
                    if xmlid:
                        sp.set("who", format_who(xmlid, role_ids, state["bad_who"]))
                    set_xmlid(sp, f"{play_id}_SP{state['sp_counter']:04d}_a")
                    label, rest = speaker_slice(text, speaker)
                    etree.SubElement(sp, q("speaker")).text = label
                    if rest.strip():
                        lg = new_lg(sp, lg_attrs, state.pop("pending_head", None) or None)
                        state["pending_head"] = None
                        etree.SubElement(lg, q("l")).text = rest.strip()
                        state["back_lg"] = lg
                    else:
                        # label-only rubric: the <lg> opens lazily with the
                        # first verse line, else it validates as empty
                        state["back_lg"] = None
                    state["back_sp"] = sp
                    state["lg"] = None
                    continue

                if is_head:
                    # a song head opens the NEXT <lg> — appending it to the
                    # open stanza puts <head> after <l>, which tei_all rejects
                    state["back_sp"] = state["back_lg"] = None
                    state["lg"] = None
                    if state["pending_head"] is not None:
                        lab = etree.SubElement(container, q("label"))
                        lab.text = state["pending_head"]
                    state["pending_head"] = stripped
                    continue

                if is_l or lg_attrs is not None:
                    # A stanza marker right after a label-only rubric <sp>
                    # opens the song INSIDE that speech; otherwise it starts a
                    # fresh block and closes any open supplement speech.
                    if lg_attrs is not None and state["back_sp"] is not None \
                            and state["back_lg"] is None:
                        state["back_lg"] = new_lg(state["back_sp"], lg_attrs,
                                                  state["pending_head"])
                        state["pending_head"] = None
                        etree.SubElement(state["back_lg"], q("l")).text = stripped
                        continue
                    if lg_attrs is not None:
                        state["back_sp"] = state["back_lg"] = None
                    if state["back_sp"] is not None:
                        if state["back_lg"] is None:
                            state["back_lg"] = new_lg(state["back_sp"], None)
                        etree.SubElement(state["back_lg"], q("l")).text = stripped
                        continue
                    if state["lg"] is None or lg_attrs is not None:
                        state["lg"] = new_lg(container, lg_attrs,
                                             state["pending_head"])
                        state["pending_head"] = None
                    l = etree.SubElement(state["lg"], q("l"))
                    l.text = stripped
                    continue

                # rubric / plain line inside supplement -> stage
                state["back_sp"] = state["back_lg"] = None
                if stripped:
                    h = etree.SubElement(container, q("stage"))
                    h.set("type", "delivery")
                    h.text = stripped
                continue

            # ---- post-trailer content: nothing but the curtain may follow a
            # trailer inside a div. The curtain slips in front of it; anything
            # else (HinkePinke's appended couplet) opens a sibling addendum div.
            if state.get("trailer_el") is not None and text.strip() and \
                    any(t in ("stage", "l", "lg", "speaker", "head") for t, _ in spans):
                is_curtain = ("stage" in tags and "פארהאנג" in
                              re.sub(r"[֑-ׇ]", "", text))
                if is_curtain:
                    a = next(at for t, at in spans if t == "stage")
                    st = etree.Element(q("stage"))
                    if a.get("type"):
                        st.set("type", a["type"])
                    st.text = text.strip()
                    state["trailer_el"].addprevious(st)
                    continue
                div = etree.SubElement(body, q("div"))
                div.set("type", "addendum")
                state["container"] = div
                state["act_div"] = div
                state["trailer_el"] = None
                # fall through: the line itself is then handled normally below


            # ---- body: speaker opens a speech ----
            speaker = next((a for t, a in spans if t == "speaker"), None)
            body_l = next((a for t, a in spans if t == "l"), None)
            body_lg_attrs = next((a for t, a in spans if t == "lg"), None)
            body_head = any(t == "head" for t, _ in spans)
            if speaker is not None:
                flush_pending_head()
                close_sp()
                state["sp_counter"] += 1
                sp = etree.SubElement(state["container"], q("sp"))
                xmlid = speaker.get("xmlid", "")
                if xmlid:
                    sp.set("who", format_who(xmlid, role_ids, state["bad_who"]))
                set_xmlid(sp, f"{play_id}_SP{state['sp_counter']:04d}_a")
                label, rest = speaker_slice(text, speaker)
                etree.SubElement(sp, q("speaker")).text = label
                if body_l is not None:
                    # sung opening line: the speech is verse, not prose
                    lg = new_lg(sp, body_lg_attrs)
                    if rest.strip():
                        etree.SubElement(lg, q("l")).text = rest.strip()
                    state["sp"], state["para"] = sp, None
                    state["body_lg"] = lg
                    continue
                p = etree.SubElement(sp, q("p"))
                state["sp"], state["para"] = sp, Para(p)
                # inline-span offsets are relative to the full line; `rest` was
                # sliced off the front, so shift them by the removed prefix.
                shift = len(text) - len(rest)
                shifted = []
                for t, a in spans:
                    if t == "speaker":
                        continue
                    a2 = dict(a)
                    off = span_int(a, "offset")
                    if off is not None:
                        if off < shift:
                            continue  # span fell inside the label/colon
                        a2["offset"] = str(off - shift)
                    shifted.append((t, a2))
                emit_line_content(state["para"], rest, shifted, skip_speaker=True,
                                  role_ids=role_ids, bad=state["bad_who"])
                continue

            # ---- body: inline song (l / lg / head spans on a body page) ----
            # Judith's corpus-wide song sweep tags sung lines `l` (+`lg`
            # stanza markers, `head` song titles) inline in the acts, not in
            # a supplement. Emit them as <lg>/<l>; without this branch these
            # lines rendered as prose or dropped (found 2026-08-02, first
            # corpus-wide build).
            if body_head and body_l is None and speaker is None:
                # song head: attaches to the next <lg>; a headless cue is
                # flushed as a musical stage direction later
                flush_pending_head()
                state["pending_head"] = stripped
                state["body_lg"] = None
                continue
            if body_l is not None or body_lg_attrs is not None:
                container = state["sp"] if state["sp"] is not None else state["container"]
                if state["body_lg"] is None or body_lg_attrs is not None:
                    state["body_lg"] = new_lg(container, body_lg_attrs)
                    if state["pending_head"] is not None:
                        h = etree.Element(q("head"))
                        h.text = state["pending_head"]
                        state["body_lg"].insert(0, h)
                        state["pending_head"] = None
                if body_l is not None:
                    off = span_int(body_l, "offset", 0)
                    ln = span_int(body_l, "length", len(text))
                    etree.SubElement(state["body_lg"], q("l")).text = \
                        text[off:off + ln].strip()
                # inline stage on a sung line ((ביס) etc.) -> sibling in the lg
                for t, a in spans:
                    if t == "stage":
                        st = etree.SubElement(state["body_lg"], q("stage"))
                        if a.get("type"):
                            st.set("type", a["type"])
                        if a.get("xmlid"):
                            st.set("who", format_who(a["xmlid"], role_ids,
                                                     state["bad_who"]))
                        soff = span_int(a, "offset", 0)
                        sln = span_int(a, "length", len(text))
                        st.text = text[soff:soff + sln].strip()
                continue
            # stage-only line while an inline song is open: keep it inside
            # the <lg> ((בּיסס), chorus cues between stanzas)
            if "stage" in tags and state["body_lg"] is not None \
                    and speaker is None:
                for t, a in spans:
                    if t != "stage":
                        continue
                    st = etree.SubElement(state["body_lg"], q("stage"))
                    if a.get("type"):
                        st.set("type", a["type"])
                    if a.get("xmlid"):
                        st.set("who", format_who(a["xmlid"], role_ids,
                                                 state["bad_who"]))
                    off = span_int(a, "offset", 0); ln = span_int(a, "length", len(text))
                    st.text = text[off:off + ln].strip()
                continue

            # ---- body: trailer (end-of-division label) ----
            if "trailer" in tags:
                flush_pending_head()
                close_sp()
                tr = etree.SubElement(state["container"], q("trailer"))
                a = next(at for t, at in spans if t == "trailer")
                if a.get("type"):
                    tr.set("type", a["type"])
                off = span_int(a, "offset", 0); ln = span_int(a, "length", len(text))
                tr.text = text[off:off + ln].strip()
                state["trailer_el"] = tr
                continue

            # ---- body: standalone stage (no open speech) ----
            if "stage" in tags and state["sp"] is None:
                flush_pending_head()
                only_stage = [s for s in spans if s[0] == "stage"]
                # one stage covering the line, or multiple: emit each
                for _, a in only_stage:
                    st = etree.SubElement(state["container"], q("stage"))
                    if a.get("type"):
                        st.set("type", a["type"])
                    xid = a.get("xmlid")
                    if xid:  # att.ascribed on <stage>
                        st.set("who", format_who(xid, role_ids, state["bad_who"]))
                    off = span_int(a, "offset", 0); ln = span_int(a, "length", len(text))
                    st.text = text[off:off + ln].strip()
                continue

            # ---- body: continuation of current speech ----
            if state["para"] is None and state["sp"] is not None and text.strip():
                # spoken line after a sung opening: open a <p> after the <lg>
                state["body_lg"] = None
                p = etree.SubElement(state["sp"], q("p"))
                state["para"] = Para(p)
                emit_line_content(state["para"], text, spans, skip_speaker=True,
                                  role_ids=role_ids, bad=state["bad_who"])
            elif state["para"] is not None:
                state["para"].lb()
                emit_line_content(state["para"], text, spans, skip_speaker=True,
                                  role_ids=role_ids, bad=state["bad_who"])
            elif text.strip() and state["act_div"] is not None:
                # orphan line INSIDE the play body (an act has opened): no open
                # speech, no speaker/stage/heading. It would vanish from the TEI
                # — record it so the linter/RA can see it (this is how untagged
                # speakers silently disappeared on Di Seder). Front matter before
                # act 1 is not "lost speech", so it's excluded.
                state["dropped"].append({"page": page_nr, "text": text.strip()[:60]})

    flush_pending_head()
    # prune structurally-empty verse groups (a label-only <sp> whose song
    # never arrived, a stray stanza marker): an <lg> without <l> is invalid
    for lg in text_el.findall(f".//{q('lg')}"):
        if lg.find(q("l")) is None and lg.find(q("lg")) is None:
            head = lg.find(q("head"))
            parent = lg.getparent()
            if head is not None and head.text:
                lab = etree.Element(q("label"))
                lab.text = head.text
                parent.replace(lg, lab)
            else:
                parent.remove(lg)
    return text_el, state["bad_who"], state["dropped"]


# --------------------------------------------------------------------------- #
#  Main
# --------------------------------------------------------------------------- #

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--play", required=True, help="data/<folder> name")
    args = ap.parse_args()

    cfg = CONFIG.get(args.play)
    if not cfg:
        raise SystemExit(f"No CONFIG block for play {args.play}")

    play_dir = REPO_ROOT / "data" / args.play
    editions = json.loads((REPO_ROOT / "data" / "editions.json").read_text())
    cast = json.loads((play_dir / "cast_dict.json").read_text())
    rec = find_edition(editions, args.play)

    pages = load_pages(play_dir)
    header, role_ids = build_header(rec, cast, cfg["play_id"])
    front = build_castlist(cast)
    text_el, bad_who, dropped = build_text(pages, cfg, role_ids)
    text_el.insert(0, front)  # <front> before <body>

    root = etree.Element(q("TEI"), nsmap=NSMAP)
    # DraCor requires TEI/@xml:id; prefixed edition slug — bare slugs like
    # "blimele"/"ezra" collide with the character xml:ids of the same name
    set_xmlid(root, f"yid-{Path(cfg['out']).stem.lower()}")
    root.append(header)
    root.append(text_el)

    tree = etree.ElementTree(root)
    model = ('<?xml-model href="http://www.tei-c.org/release/xml/tei/custom/'
             'schema/relaxng/tei_all.rng" type="application/xml" '
             'schematypens="http://relaxng.org/ns/structure/1.0"?>')
    out_path = REPO_ROOT / cfg["out"]
    xml_bytes = etree.tostring(tree, pretty_print=True, xml_declaration=True,
                               encoding="UTF-8")
    # inject the model PI after the XML declaration
    head, _, body = xml_bytes.partition(b"\n")
    out_path.write_bytes(head + b"\n" + model.encode() + b"\n" + body)

    # DraCor variant: identical except <fw> stripped (printed page furniture —
    # dracor.rng forbids it; the canonical tei_all edition keeps it)
    import copy
    droot = copy.deepcopy(root)
    for fw in droot.findall(f".//{q('fw')}"):
        fw.getparent().remove(fw)
    dr_path = REPO_ROOT / "tei" / "dracor" / Path(cfg["out"]).name
    dr_path.parent.mkdir(exist_ok=True)
    dr_path.write_bytes(etree.tostring(etree.ElementTree(droot), pretty_print=True,
                                       xml_declaration=True, encoding="UTF-8"))
    print(f"Wrote {dr_path} (DraCor variant)")

    # report
    n_sp = len(root.findall(f".//{q('sp')}"))
    n_stage = len(root.findall(f".//{q('stage')}"))
    n_lg = len(root.findall(f".//{q('lg')}"))
    acts = root.findall(f".//{q('div')}[@type='act']")
    print(f"Wrote {out_path}")
    print(f"  acts={len(acts)} sp={n_sp} stage={n_stage} lg={n_lg} "
          f"persons={len(role_ids)}")
    if bad_who:
        print(f"  WARNING bad @who (not in cast_dict): {sorted(set(bad_who))}")
    else:
        print("  all @who reference declared roles")
    if dropped:
        print(f"  WARNING {len(dropped)} orphan line(s) dropped (no sp/stage/heading) — "
              "likely untagged speakers; run lint_pages.py:")
        for d in dropped[:10]:
            print(f"    p{d['page']}: {d['text']!r}")
        if len(dropped) > 10:
            print(f"    … and {len(dropped) - 10} more")


if __name__ == "__main__":
    main()
