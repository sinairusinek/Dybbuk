"""The work layer: WEMI Works, the songs inside them, and performance events.

`build_entity_graph.py` originally had four node kinds — edition, person, org,
place — and no work. Editions stood in for plays, which made the graph assert
false things: 42 performance edges claimed a performance of a book that did not
yet exist (Mishke Mashke performed 1889, printed 1911). 82% of all edges were
work-level but hung off editions, and the 252 catalogued plays with no surviving
edition could not be represented at all.

This module supplies the missing layers. See `YiDraCor/docs/work_layer_proposal.md`
for the full argument and the decisions behind each choice.

  work    one per catalogued play, keyed `work:<Expression ID>`. All 277 are
          minted — including the 252 with no surviving text — so a poster for a
          lost play has something to attach to.
  song    one per song attested in a play. Songs are Works in their own right
          (`part_of` a play work), not attributes of it: they have their own
          sheet music, recordings and composers.
  event   one performance event, when the evidence names a specific staging.

Decisions recorded by Sinai 2026-10-05, all implemented here:
  * mint all 277 works, text or no text;
  * mint falsely-ascribed works too, carrying the attribution status as such;
  * carry `certainty`'s five raw values verbatim — `error` and
    `false ascription` are different claims — and give Hurwitz `unknown`
    rather than a defaulted `certain` nobody checked;
  * union both sheets' schemas, recording which sheet each field came from so a
    null reads as "unrecorded here", never "none existed";
  * songs get minted ids: the catalogue's `tempID` is explicitly temporary and
    not even unique (216 distinct over 224 rows);
  * cast facts are recordable at either granularity — see `coarse_to_fine`.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent          # YiDraCor/
CATALOGUE = ROOT / "edition metadata" / "DybbukCatalogue May2024.xlsx"
SONG_IDS = ROOT / "data" / "song_ids.tsv"

PLAY_SHEETS = {"Lateiner Plays": "Lateiner", "Hurwitz Plays": "Hurwitz"}
SONG_SHEET = "songs_Lateiner and Hurwitz"

# The 9 columns both Plays sheets share. These carry the whole work identity;
# everything else is secondary and present for only one playwright.
SHARED_COLS = ("Expression ID", "English Name", "Yiddish Name", "author",
               "TAGS", "Genre", "certainty", "expression", "comments")

# `certainty` is carried through verbatim: five raw values, no normalisation.
# `error` is NOT `false ascription` — 3841 Di laykhtziniger is a misattribution
# (the play exists, by Friendsel) while 3907 Nekhemye kugl is a spurious title,
# not an independent work at all. Collapsing them would destroy that.
CERTAINTY_VALUES = {"certain", "uncertain", "false ascription", "error",
                    "uncertain/false?"}
CERTAINTY_UNKNOWN = "unknown"

# Confirmed human merges. Each pair is ONE play that the workbook lists on both
# playwrights' sheets — not two catalogue rows for one play, so merging settles
# an authorship question and the evidence decides it, not the lower id.
#
# reviewer: Sinai 2026-10-06
WORK_MERGE = {
    # Di tsigaynerin. The Lateiner row is explicitly `false ascription`:
    # "This title was found only in Berkovitsh's book. This play is by
    # M. Horowitz", citing an 1888 notice
    # (nli.org.il/he/newspapers/flkadv/1888/12/07/01/article/24.4).
    "3850": "3952",
    # Virdzhinus. The Hurwitz row carries the evidence — a November 1897 Thalia
    # advertisement with Kessler in the title role — while the Lateiner row says
    # only "This title is found only in the daily press". The Hurwitz sheet's own
    # "Is this Hurwitz?" note means this one stays reviewable.
    "3920": "3993",
}
WORK_MERGE_REVIEWER = "Sinai 2026-10-06"


# ---------------------------------------------------------------------------
# normalisation
# ---------------------------------------------------------------------------
def expression_id(v) -> str:
    """Expression ID as a string.

    openpyxl returns numeric cells as floats, so an id arrives as `4010.0`.
    **Never use `rstrip('.0')`** — rstrip strips a character SET, so `'4010.0'`
    becomes `'401'` and `'3830.0'` becomes `'383'`. 28 of the 277 work ids end
    in zero; that bug once made me report Ben Hador and Emigration as missing
    from the catalogue when both were present.
    """
    if v is None:
        return ""
    s = str(v).strip()
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def clean(v) -> str:
    """Spreadsheet cell -> a value safe for JSON and TSV."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "??", "?", "-"}:
        return ""
    # Collapse internal runs of whitespace too: the catalogue has
    # "Di  Tsigaynerin" with a double space, which would otherwise read as a
    # different title from "Di tsigaynerin".
    return re.sub(r"\s+", " ", re.sub(r"[\t\r\n]+", " ", s))


_POINTS = re.compile(r"[֑-ׇ]")


def sm(s) -> str:
    """Normalise for matching: NFKD first, THEN strip points."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = _POINTS.sub("", s)
    return re.sub(r"[^\w֐-׿]+", "", s, flags=re.UNICODE).casefold()


# Yiddish romanisation is unstandardised across these sheets, so the same play
# is spelled many ways (Khinke/Hinke Pinke, seyder/Seder, Ishe-roe/Isha Raa).
# These folds mirror DybbukMedia/code/common.py:translit_key, which is pinned by
# a regression test at 26 real spellings and 12 control titles.
_FOLD = [
    (r"w|v", "u"), (r"[aeiouy]+", "a"), (r"kh|ch|h", "x"), (r"ts|tz|c", "ts"),
    (r"sh|sch", "sh"), (r"ss|s|z", "s"), (r"nn|n", "n"), (r"mm|m", "m"),
    (r"ll|l", "l"), (r"bb|b|p|pp|f|ff|ph", "b"), (r"dd|d|t|tt", "d"),
    (r"gg|g|k|kk|q", "g"), (r"rr|r", "r"),
]


def translit_key(s) -> str:
    """Collapse a romanised title to a comparison skeleton."""
    s = re.sub(r"[(\)\[][^()\[\]]*[()\]]", " ", str(s or ""))
    s = re.sub(r"t\b", "s", s, flags=re.I)            # Naharot / Nahares
    s = re.sub(r"\b(?:und|un|and|der|di|dos|das|de|the|a|an|oder|odr|or)\b",
               " ", s, flags=re.I)
    base = sm(s)
    if not base or any("֐" <= c <= "׿" for c in base):
        return base
    out = re.sub(r"[^a-z]", "", base)
    for pat, rep in _FOLD:
        out = re.sub(pat, rep, out)
    return re.sub(r"(.)\1+", r"\1", out)


# ---------------------------------------------------------------------------
# works
# ---------------------------------------------------------------------------
def load_works(wb) -> dict:
    """Every catalogued play as a work node, keyed `work:<Expression ID>`.

    All 277 are minted, including the 252 with no surviving edition — that is
    what makes a poster for a lost play attachable. Falsely-ascribed works are
    minted too, carrying their attribution status rather than being omitted:
    a false ascription is evidence about reception history.
    """
    works: dict[str, dict] = {}
    for sheet, playwright in PLAY_SHEETS.items():
        rows = list(wb[sheet].iter_rows(values_only=True))
        header = [clean(c) for c in rows[0]]
        for row in rows[1:]:
            if not any(c is not None for c in row):
                continue
            cells = {header[i]: row[i] for i in range(min(len(header), len(row)))
                     if header[i]}
            wid = expression_id(cells.get("Expression ID"))
            if not wid:
                continue

            # Union of both schemas. A null here means "unrecorded in this
            # sheet", never "none existed" — Hurwitz Plays has a Composer column
            # and Lateiner Plays does not, yet Mogulesco demonstrably composed
            # for Lateiner. `field_sources` keeps that readable.
            extras = {k: clean(v) for k, v in cells.items()
                      if k not in SHARED_COLS and clean(v)}

            cert = clean(cells.get("certainty")).lower()
            if cert not in CERTAINTY_VALUES:
                # Hurwitz Plays' certainty column is empty for all 125 rows.
                # `unknown` says so; defaulting to `certain` would assert
                # something nobody checked.
                cert = CERTAINTY_UNKNOWN

            node = {
                "id": f"work:{wid}",
                "kind": "work",
                "label": clean(cells.get("English Name")),
                "yiddish_title": clean(cells.get("Yiddish Name")),
                "expression_id": wid,
                "playwright": playwright,
                "author_raw": clean(cells.get("author")),
                "tags": clean(cells.get("TAGS")),
                "genre": clean(cells.get("Genre")),
                "attribution": cert,
                "attribution_note": clean(cells.get("comments")),
                # Adaptation lineage points OUTSIDE the corpus (Dumas,
                # Strindberg, Goldfaden, Nordau), and the values are hedged
                # prose, so they are carried as text. Parsing them into
                # entities is a research task, deferred by decision.
                "adapted_from_raw": clean(cells.get("expression")),
                "source_sheet": sheet,
                "extras": extras,
                "status": "LINKED",
                "method": "catalogue_expression_id",
            }
            if wid in WORK_MERGE:
                # Keep the node as a tombstone so a reference to the retired id
                # still resolves, and so the superseded attribution stays
                # visible as evidence about the dispute rather than vanishing.
                node["merged_into"] = f"work:{WORK_MERGE[wid]}"
                node["status"] = "MERGED"
                node["reviewer"] = WORK_MERGE_REVIEWER
            if wid in works:
                # Verified: no Expression ID appears in both sheets. If that
                # ever changes, fail loudly rather than silently overwriting.
                raise ValueError(
                    f"Expression ID {wid} in both {works[wid]['source_sheet']} "
                    f"and {sheet} — the work key is no longer unique")
            works[wid] = node
    return works


def work_title_index(works: dict) -> dict:
    """skeleton -> [expression_id], over English and Yiddish titles."""
    idx: dict[str, list] = {}
    for wid, w in works.items():
        if w.get("merged_into"):
            continue          # a retired id must never win a title match
        for cand in (w["label"], w["yiddish_title"]):
            key = translit_key(cand)
            if len(key) < 4:
                continue
            idx.setdefault(key, [])
            if wid not in idx[key]:
                idx[key].append(wid)
    return idx


def resolve_work(idx: dict, text, works: dict | None = None) -> tuple[str, str]:
    """Resolve a play title to one work. Ambiguity is a GAP, never a coin toss.

    The songs sheet writes play keys in short form while the catalogue keeps the
    full `X oder Y` title — `Yafes Toyar` for "Yafes toyer oder, Bilem haroshe",
    `Bas kohen` for "Bas Cohen oder, Malka Alexandra". So an exact skeleton miss
    falls back to a prefix match, which must stay unambiguous to count.

    When several hits all resolve to the SAME expression id they are one work
    under two spellings (the catalogue has a `Di tsigaynerin` / `Di  Tsigaynerin`
    pair differing only by a double space), so that is not ambiguity.
    """
    key = translit_key(text)
    if len(key) < 4:
        return "", "GAP"

    def pick(hits: list) -> str:
        if len(hits) == 1:
            return hits[0]
        if len(hits) > 1 and works:
            # Duplicate work rows for one play: identical labels under two
            # expression ids. Prefer the lowest id so the choice is stable, and
            # only when the labels really agree.
            labels = {translit_key(works[h]["label"]) for h in hits}
            if len(labels) == 1:
                return sorted(hits, key=lambda h: int(h) if h.isdigit() else 0)[0]
        return ""

    nid = pick(idx.get(key) or [])
    if nid:
        return nid, "PROPOSED"

    # Prefix fallback, long enough that a short title cannot swallow a longer
    # unrelated play.
    if len(key) >= 6:
        pref = [n for k, v in idx.items() if k.startswith(key) for n in v]
        nid = pick(sorted(set(pref)))
        if nid:
            return nid, "PROPOSED"
    return "", "GAP"


# ---------------------------------------------------------------------------
# songs
# ---------------------------------------------------------------------------
def load_song_ids() -> dict:
    """Persisted song ids: signature -> song_id.

    Songs need MINTED ids — the catalogue's `tempID` is explicitly temporary and
    not unique (216 distinct values over 224 populated rows). Ids are therefore
    assigned once and persisted, never renumbered, so that a human decision
    attached to `song:7` still means the same song after the next rebuild.
    """
    if not SONG_IDS.exists():
        return {}
    out = {}
    for line in SONG_IDS.read_text(encoding="utf-8").splitlines()[1:]:
        if not line.strip():
            continue
        sid, sig = line.split("\t", 1)
        out[sig] = sid
    return out


def save_song_ids(mapping: dict) -> None:
    """Write the id ledger back, sorted by numeric id."""
    SONG_IDS.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(mapping.items(), key=lambda kv: int(kv[1].split(":")[-1]))
    body = "\n".join(f"{sid}\t{sig}" for sig, sid in rows)
    SONG_IDS.write_text("song_id\tsignature\n" + body + "\n", encoding="utf-8")


def song_signature(play_key: str, title: str) -> str:
    """Stable identity for a song: its play plus its normalised title.

    Deliberately not the row position — rows get reordered. Not `tempID` either,
    since that is temporary and non-unique.
    """
    return f"{translit_key(play_key)}|{sm(title)}"


def load_songs(wb, work_idx: dict, works: dict) -> tuple[list, dict]:
    """Songs as Work nodes that are `part_of` a play work.

    A song is a Work in its own right, not an attribute of the play: 57 rows
    come from `Shund on Shellac` (recordings) and 64 carry recording counts, so
    a song has its own editions (sheet music) and its own performances. Modelled
    `song --part_of--> work`.

    Returns (nodes, id_mapping).
    """
    rows = list(wb[SONG_SHEET].iter_rows(values_only=True))
    header = [clean(c) for c in rows[0]]
    ids = load_song_ids()
    next_n = max((int(v.split(":")[-1]) for v in ids.values()), default=0) + 1

    # Identity is (play, title) — the PLAY is what matters; the publication a
    # song happens to be printed in is bibliography, never a parent.
    #
    # Two consequences, both deliberate:
    #  * One song listed under the SAME play in several songbooks is ONE node.
    #    תקיעה גדולה is printed in both `Di yidishe bihne` and `Shund on
    #    Shellac`; those are secondary sources, so they become `attestations`
    #    on the single node and never nodes of their own.
    #  * One TITLE under DIFFERENT plays stays SEPARATE nodes. 17 titles do
    #    this, and they are mostly form-names reused across plays — דועט
    #    (Duet) appears in Der kuzari, Ben Hador and Ishe roeh, טערצעט in two
    #    more. Merging them would invent a travelling song that the sources do
    #    not attest.
    grouped: dict[str, list] = {}
    order: list[str] = []
    skipped: list[dict] = []
    for row in rows[1:]:
        if not any(c is not None for c in row):
            continue
        cells = {header[i]: row[i] for i in range(min(len(header), len(row)))
                 if header[i]}
        play_key = clean(cells.get("Play Key"))
        # `כלל יידיש` (normalised Yiddish) is populated for all 235 rows, so it
        # is the most reliable title; the source spelling is kept beside it.
        title_yi = clean(cells.get("כלל יידיש"))
        title_src = clean(cells.get("Song title in sources"))
        title_yivo = clean(cells.get("YIVO transliteration"))
        if not (title_yi or title_src or title_yivo):
            # Two rows carry no title in any of the three columns: one has only
            # a romanized form ("Awojde", Beys Dovid) and one is a
            # collection-level row naming no play at all. Reported, not dropped
            # silently.
            skipped.append({k: clean(v) for k, v in cells.items() if clean(v)})
            continue

        sig = song_signature(play_key, title_yi or title_src or title_yivo)
        if sig not in grouped:
            grouped[sig] = []
            order.append(sig)
        grouped[sig].append(cells)

    nodes = []
    for sig in order:
        rows_for_song = grouped[sig]
        cells = rows_for_song[0]
        play_key = clean(cells.get("Play Key"))
        title_yi = clean(cells.get("כלל יידיש"))
        title_src = clean(cells.get("Song title in sources"))
        title_yivo = clean(cells.get("YIVO transliteration"))

        if sig not in ids:
            ids[sig] = f"song:{next_n}"
            next_n += 1

        wid, status = resolve_work(work_idx, play_key, works)
        # Prefer a row that actually credits an author over one that does not:
        # of two attestations of תקיעה גדולה, only the Shund on Shellac row
        # names Lateiner.
        author = ""
        for c in rows_for_song:
            a = clean(c.get("Author"))
            if a and a.lower() not in {"not known", "unkown", "unknown"}:
                author = a
                break
        attestations = [{
            "source_publication": clean(c.get("Source")),
            "page_pdf": clean(c.get("Page number PDF")),
            "page_printed": clean(c.get("page numbers")),
            "external_source_id": clean(c.get("external source id")),
            "recordings": clean(c.get("recordings acc. Shund on Shellac")),
            "author_raw": clean(c.get("Author")),
            "temp_id": clean(c.get("tempID")),
        } for c in rows_for_song]
        nodes.append({
            "id": ids[sig],
            "kind": "song",
            "label": title_yivo or title_src or title_yi,
            "yiddish_title": title_yi,
            "title_in_source": title_src,
            "romanized_title": clean(cells.get("Romanized title")),
            "play_key": play_key,
            "work_id": f"work:{wid}" if wid else "",
            # `Author` conflates lyricist and composer, and 12 rows say
            # "Not known"/"Unkown" — normalised to the same `unknown` used for
            # Hurwitz attribution rather than silently dropped.
            "author_raw": ("" if author.lower() in {"not known", "unkown",
                                                    "unknown"} else author),
            "author_certainty": (CERTAINTY_UNKNOWN
                                 if author.lower() in {"not known", "unkown",
                                                       "unknown"} or not author
                                 else "stated"),
            "source_publication": clean(cells.get("Source")),
            "external_source_id": clean(cells.get("external source id")),
            "recordings": clean(cells.get("recordings acc. Shund on Shellac")),
            "attestations": attestations,
            "n_attestations": len(attestations),
            "status": "LINKED" if wid else "GAP",
            "method": "catalogue_song_sheet" if wid else "",
            "link_status_to_work": status,
        })
    if skipped:
        print(f"  songs: {len(skipped)} row(s) with no title in any column, "
              f"skipped: "
              + "; ".join(x.get("Play Key", "(no play)")[:40] for x in skipped))
    return nodes, ids


# ---------------------------------------------------------------------------
# the coarse-to-fine rule
# ---------------------------------------------------------------------------
def coarse_to_fine(work_level: list, event_level: list) -> list:
    """Drop work-level cast claims that an event-level claim already covers.

    Sinai's requirement: "many cases we will know that an actor played a role in
    a play without knowledge of the exact performance events, but when we have
    the refined data we keep it."

    So both granularities coexist. `person --actor--> work` stands on its own
    evidence (the Leksikon attests the role) and is NOT a placeholder to delete.
    What must not happen is double-counting: when the same person, role and work
    are attested at the event level too, the event-level fact subsumes the
    work-level one for counting purposes.

    The reverse inference is invalid and not performed anywhere: an event-level
    fact implies the work-level one, but a work-level fact must never be
    promoted into an invented event.
    """
    covered = {(e.get("src"), e.get("rel"), e.get("character"), e.get("work"))
               for e in event_level}
    out = []
    for e in work_level:
        key = (e.get("src"), e.get("rel"), e.get("character"), e.get("dst"))
        if key in covered:
            continue
        out.append(e)
    return out
