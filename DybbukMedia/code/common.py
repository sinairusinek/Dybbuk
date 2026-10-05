"""Shared helpers for the DybbukMedia harvesters.

The manifest is append-and-update-by-media_id. Nothing here ever truncates it:
it accumulates human decisions (link_status promotions, reviewer stamps, crops)
that no generator can reproduce. See docs/SCHEMA.md.
"""
from __future__ import annotations

import csv
import json
import re
import unicodedata
from pathlib import Path

csv.field_size_limit(10 ** 7)

ROOT = Path(__file__).resolve().parent.parent      # DybbukMedia/
REPO = ROOT.parent                                  # Dybbuk/
MANIFEST = ROOT / "data" / "media_manifest.tsv"
ENTITY_GRAPH = REPO / "YiDraCor" / "data" / "entity_graph.json"
CATALOGUE = REPO / "YiDraCor" / "edition metadata" / "DybbukCatalogue May2024.xlsx"
ALBUM_DB = REPO / "Zylbercweig" / "TheAlbum" / "build" / "tagger" / "album.db"

COLUMNS = [
    "media_id", "source", "item_type",
    "title", "title_yiddish", "date", "date_note", "place",
    "holding_institution", "collection", "signature",
    "source_url", "fetch_hint", "local_path", "rights",
    "entity_ids", "play_key", "expression_id", "link_status", "reviewer",
    "status", "crop_of", "notes",
]

# Columns a human owns. A re-run must never overwrite a non-empty value here.
HUMAN_OWNED = {"entity_ids", "link_status", "reviewer", "local_path",
               "status", "crop_of", "notes"}


# --------------------------------------------------------------------------
# normalisation
# --------------------------------------------------------------------------
_POINTS = re.compile(r"[֑-ׇ]")          # Hebrew points & cantillation
_PUNCT = re.compile(r"[^\w֐-׿]+", re.UNICODE)


def sm(s) -> str:
    """Normalise for matching: NFKD first, THEN strip points. Order matters —
    precomposed forms hide their points from the regex otherwise."""
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = _POINTS.sub("", s)
    s = _PUNCT.sub("", s)
    return s.casefold()


def strip_parens(s: str) -> str:
    """Drop (qualifiers) — including the RTL-mangled )form( — from both sides
    of a comparison."""
    s = re.sub(r"[(\)][^()]*[()]", " ", str(s or ""))
    return re.sub(r"\s+", " ", s).strip()


def clean(v) -> str:
    """Spreadsheet cell -> manifest cell. Tabs and newlines would corrupt a TSV."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"nan", "none", "??", "?"}:
        return ""
    return re.sub(r"[\t\r\n]+", " ", s)


def year_of(v) -> str:
    """Pull a 4-digit year or a YYYY-YYYY range out of free text."""
    s = clean(v)
    if not s:
        return ""
    rng = re.search(r"\b(1[89]\d{2})\s*[-–]\s*(1[89]\d{2})\b", s)
    if rng:
        return f"{rng.group(1)}-{rng.group(2)}"
    one = re.search(r"\b(1[89]\d{2})\b", s)
    return one.group(1) if one else ""


# --------------------------------------------------------------------------
# entity graph
# --------------------------------------------------------------------------
def work_of_edition(ents: dict) -> dict:
    """edition node id -> the work it realises, from the `realised_as` spine."""
    out = {}
    for e in ents.get("edges", []):
        if e.get("rel") == "realised_as":
            out[e["dst"]] = e["src"]
    return out


def load_entities() -> dict:
    """Index entity_graph.json nodes by normalised label and by db_id.

    Returns {"by_norm": {norm_label: [node_id]}, "nodes": {id: node}}.
    A normalised label may map to several nodes — that ambiguity is reported,
    never silently resolved.
    """
    g = json.loads(ENTITY_GRAPH.read_text(encoding="utf-8"))
    nodes, by_norm = {}, {}
    for n in g["nodes"]:
        nodes[n["id"]] = n
        for key in (n.get("label"), n.get("matched"), n.get("yiddish_title")):
            if not key:
                continue
            for form in {sm(key), sm(strip_parens(key))}:
                if len(form) < 3:
                    continue
                by_norm.setdefault(form, [])
                if n["id"] not in by_norm[form]:
                    by_norm[form].append(n["id"])
    out = {"by_norm": by_norm, "nodes": nodes, "edges": g.get("edges", [])}
    out["_work_of"] = work_of_edition(out)
    return out


def propose(ents: dict, text, kinds=None) -> tuple[str, str]:
    """Look `text` up in the entity index.

    Returns (entity_ids, link_status). Only an unambiguous single hit becomes a
    PROPOSED anchor — several equally good candidates is a GAP for a human,
    exactly as the entity graph treats them. Never returns LINKED: that status
    is a human's to grant.
    """
    for form in (sm(text), sm(strip_parens(text))):
        if len(form) < 3:
            continue
        hits = ents["by_norm"].get(form)
        if not hits:
            continue
        if kinds:
            hits = [h for h in hits if ents["nodes"][h].get("kind") in kinds]
        if len(hits) == 1:
            return hits[0], "PROPOSED"
        if len(hits) > 1:
            # Several labels can be spelling variants of ONE real entity: the
            # graph holds both "Windsor Theater" and "Windsor Theatre", each
            # carrying db_id 134. Agreeing db_ids is not ambiguity, so collapse
            # on db_id and keep the node whose own status is strongest.
            dbids = {str(ents["nodes"][h].get("db_id") or "") for h in hits}
            if len(dbids) == 1 and "" not in dbids:
                best = sorted(hits, key=lambda h: (
                    ents["nodes"][h].get("method") != "reviewed", h))
                return best[0], "PROPOSED"
            return "", "GAP"
    return "", "GAP"


# --------------------------------------------------------------------------
# manifest I/O
# --------------------------------------------------------------------------
def read_manifest() -> dict:
    """media_id -> row dict. Missing file is an empty manifest, not an error."""
    if not MANIFEST.exists():
        return {}
    with MANIFEST.open(encoding="utf-8", newline="") as fh:
        return {r["media_id"]: r for r in csv.DictReader(fh, delimiter="\t")
                if r.get("media_id")}


def reconcile_link_status(rows: dict) -> int:
    """Keep `link_status` consistent with whether a row actually has an anchor.

    `entity_ids` and `link_status` are both human-owned, which means a re-run can
    fill an empty `entity_ids` while leaving a stale `GAP` in place — exactly
    what happened when the work layer gave 388 previously-unanchorable rows an
    anchor. The pair is one fact, so it is reconciled rather than left to drift:
    a row that gained an anchor becomes PROPOSED unless a human had already
    promoted it, and a row that lost one becomes GAP.

    A human's LINKED is never downgraded while the anchor stands.
    """
    fixed = 0
    for row in rows.values():
        has = bool(row.get("entity_ids"))
        status = row.get("link_status") or ""
        if has and status == "GAP":
            row["link_status"] = "PROPOSED"
            fixed += 1
        elif not has and status != "GAP":
            row["link_status"] = "GAP"
            row["reviewer"] = ""
            fixed += 1
    return fixed


def write_manifest(rows: dict) -> None:
    """Write every row under the canonical header, sorted by media_id.

    Enforcing COLUMNS at write time is deliberate: a save that silently drops
    schema columns has bitten this project before.
    """
    fixed = reconcile_link_status(rows)
    if fixed:
        print(f"  reconciled link_status on {fixed} row(s)")
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    def sort_key(mid: str):
        m = re.match(r"(.*?)-(\d+)$", mid)
        return (m.group(1), int(m.group(2))) if m else (mid, 0)
    with MANIFEST.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t",
                           extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        for mid in sorted(rows, key=sort_key):
            w.writerow({c: rows[mid].get(c, "") for c in COLUMNS})


def merge(existing: dict, new_rows: list[dict]) -> tuple[int, int, int]:
    """Merge generated rows into the manifest.

    Returns (added, updated, protected): `protected` counts human-owned fields
    a re-run wanted to change and was refused.
    """
    added = updated = protected = 0
    for row in new_rows:
        mid = row["media_id"]
        if mid not in existing:
            existing[mid] = {c: row.get(c, "") for c in COLUMNS}
            added += 1
            continue
        cur, touched = existing[mid], False
        for col, val in row.items():
            if col not in COLUMNS or col == "media_id" or not val:
                continue
            if col in HUMAN_OWNED and cur.get(col):
                if cur[col] != val:
                    protected += 1
                continue
            if cur.get(col) != val:
                cur[col] = val
                touched = True
        updated += touched
    return added, updated, protected


# --------------------------------------------------------------------------
# cross-script person bridging
# --------------------------------------------------------------------------
PEOPLE_DB = REPO / "Zylbercweig" / "people" / "people_db.tsv"


def _token_set(s: str) -> frozenset:
    """Order-insensitive token set, locators and parentheticals removed.

    The catalogue writes `Surname, Given (Alias) 936:1`; people_db writes
    `Given Surname (Alias)`. Only a token set matches both.
    """
    s = re.sub(r"\b\d+:\d+\b", " ", str(s or ""))
    s = strip_parens(s)
    toks = {sm(t) for t in re.split(r"[\s,;|]+", s)}
    return frozenset(t for t in toks if len(t) > 1)


def load_person_bridge() -> dict:
    """Index people_db so a name in ANY script resolves to a db_id.

    The Album names people in Latin script while the entity graph mostly holds
    Yiddish `Surname, Given`. people_db carries both (`english`, `hebname`,
    `name_variants`) under one `db_id`, so it is the only thing that can join
    them. Returns {"exact": {norm: db_id}, "tokens": {frozenset: {db_id}}}.
    """
    exact: dict[str, str] = {}
    tokens: dict[frozenset, set] = {}
    if not PEOPLE_DB.exists():
        return {"exact": exact, "tokens": tokens}
    with PEOPLE_DB.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            db_id = (row.get("db_id") or "").strip()
            if not db_id:
                continue
            forms = [row.get("hebname"), row.get("english"),
                     row.get("alternative_name")]
            forms += (row.get("name_variants") or "").split("|")
            for form in forms:
                form = (form or "").strip()
                if len(form) < 3:
                    continue
                for key in {sm(form), sm(strip_parens(form))}:
                    if len(key) >= 3:
                        # First writer wins; a later homonym must not silently
                        # steal an established name form.
                        exact.setdefault(key, db_id)
                ts = _token_set(form)
                if len(ts) >= 2:
                    tokens.setdefault(ts, set()).add(db_id)
    return {"exact": exact, "tokens": tokens}


def db_id_for_person(bridge: dict, name) -> str:
    """Resolve a person name of any script to a people_db db_id, or ""."""
    name = clean(name)
    if len(name) < 3:
        return ""
    for key in (sm(name), sm(strip_parens(name))):
        if key in bridge["exact"]:
            return bridge["exact"][key]
    ts = _token_set(name)
    if len(ts) >= 2:
        hit = bridge["tokens"].get(ts)
        if hit and len(hit) == 1:
            return next(iter(hit))
    return ""


def graph_person_by_db_id(ents: dict) -> dict:
    """db_id -> entity-graph node id, for the person nodes that carry one."""
    out = {}
    for nid, n in ents["nodes"].items():
        if n.get("kind") == "person" and n.get("db_id"):
            out.setdefault(str(n["db_id"]), nid)
    return out


# --------------------------------------------------------------------------
# transliteration-tolerant title matching
# --------------------------------------------------------------------------
# Yiddish romanisation is unstandardised across these sources, so the same play
# appears as Khinke/Hinke Pinke, seyder/Seder, Egypten/Egipten, Ishe-roe/Isha
# Raa. Folding the systematic alternations to one skeleton bridges them without
# the false positives a pure edit-distance threshold would admit.
_TRANSLIT_FOLD = [
    (r"kh|ch|h", "x"),        # khurbn / hurbn / churbn
    (r"ts|tz|c", "ts"),       # tsvey / tzvey
    (r"sh|sch", "sh"),
    # seyder/seider/Seder must land together, so the ey/ei digraphs fold to the
    # same vowel as a bare e rather than to i.
    (r"ei|ey|ay|ai", "a"),
    (r"oy|oi|au", "oy"),
    (r"w|v", "u"),            # Varshe / Warsha
    # Every vowel folds to one class: romanisers disagree on vowel QUALITY far
    # more than on consonants (Kidush/Kidesh, Nahares/Naharot, roe/raa), so
    # distinguishing them loses more true matches than it prevents false ones.
    (r"[aeiouy]+", "a"),
    (r"ss|s|z", "s"),
    (r"nn|n", "n"), (r"mm|m", "m"), (r"ll|l", "l"),
    (r"bb|b|p|pp|f|ff|ph", "b"),
    (r"dd|d|t|tt", "d"),      # (tav/sav handled below, before this fold)
    (r"gg|g|k|kk|q", "g"),
    (r"rr|r", "r"),
]


def translit_key(s) -> str:
    """Collapse a romanised Yiddish title to a comparison skeleton.

    Latin script only — a Hebrew-script string is returned as its own sm() form,
    since these folds are about romanisation choices, not Yiddish orthography.
    """
    s = strip_parens(str(s or ""))
    # Ashkenazi vs Israeli Hebrew renders tav as s or t (Naharot / Nahares,
    # Bas / Bat). Fold a word-final tav here, on the raw string: sm() strips
    # spaces, so afterwards there is no word boundary left to anchor on.
    s = re.sub(r"t\b", "s", s, flags=re.I)
    # Conjunctions and articles are written, dropped or hyphenated freely across
    # these sources ("Yaakov und Esav" / "Yakov un Eysev" / "Yaakov-Esav"), so
    # they carry no matching signal.
    s = re.sub(r"\b(?:und|un|and|der|di|dos|das|de|the|a|an)\b", " ", s,
               flags=re.I)
    base = sm(s)
    if not base or any("֐" <= c <= "׿" for c in base):
        return base
    out = re.sub(r"[^a-z]", "", base)
    for pat, rep in _TRANSLIT_FOLD:
        out = re.sub(pat, rep, out)
    out = re.sub(r"(.)\1+", r"\1", out)       # collapse doubles last
    return out


def load_title_index(ents: dict) -> dict:
    """skeleton -> [node_id] over work and edition titles.

    Works come first and win: a poster advertises a PLAY, not a particular
    printing, so `work:*` is the right anchor for a play title. The edition is
    reachable from the work through `realised_as`. Editions stay in the index
    for the items that genuinely describe one — a title page, a page scan.
    """
    idx: dict[str, list] = {}
    by_work = work_of_edition(ents)
    for kind in ("work", "edition"):
      for nid, n in ents["nodes"].items():
        if n.get("kind") != kind:
            continue
        cands = [n.get("label"), n.get("yiddish_title"),
                 re.sub(r"[_-].*$", "", str(n.get("folder") or ""))]
        for c in cands:
            key = translit_key(c)
            if len(key) < 4:
                continue
            # A play title always anchors to the WORK, never to a printing: an
            # edition's own title spellings are therefore indexed under the work
            # it realises. Otherwise the anchor would depend on which spelling a
            # given source sheet happened to use, which is arbitrary.
            target = by_work.get(nid, nid) if kind == "edition" else nid
            idx.setdefault(key, [])
            if target not in idx[key]:
                idx[key].append(target)
    return idx


def propose_title(ents: dict, idx: dict, text) -> tuple[str, str]:
    """Anchor a play/edition title, tolerating romanisation differences.

    Tries the exact label index first, then the transliteration skeleton. A
    skeleton hitting several editions is a GAP, not a coin toss.
    """
    nid, status = propose(ents, text, kinds={"work", "edition"})
    if nid:
        # A play title anchors to the work even when it matched an edition's
        # own label exactly: the edition is reachable via `realised_as`, and the
        # anchor must not depend on which spelling a source sheet used.
        return ents.get("_work_of", {}).get(nid, nid), status
    key = translit_key(text)
    if len(key) < 4:
        return "", "GAP"
    def pick(hits):
        """A duplicate-marked node must never win over the real edition."""
        real = [h for h in hits
                if "DUPLICATE" not in str(ents["nodes"][h].get("label") or "")]
        return real[0] if len(real) == 1 else ""

    nid = pick(idx.get(key) or [])
    if nid:
        return nid, "PROPOSED"

    # These titles carry an `oder`/`or` alternative title that the db records
    # under the main title alone ("Kidesh Hashem, oder der yidisher minister").
    # Try each side of the disjunction, longest first.
    parts = re.split(r"\b(?:oder|odr|or|ader)\b", str(text or ""), flags=re.I)
    if len(parts) > 1:
        for part in sorted((p.strip(" ,.:;-") for p in parts), key=len,
                           reverse=True):
            k = translit_key(part)
            if len(k) < 4:
                continue
            nid = pick(idx.get(k) or [])
            if nid:
                return nid, "PROPOSED"

    # Otherwise a unique edition whose skeleton this title starts with, which
    # catches "Emigration" -> "Emigration nach America". Require a substantial
    # prefix so short titles cannot swallow unrelated editions.
    if len(key) >= 7:
        pref = [n for k, v in idx.items()
                if k.startswith(key) or (len(k) >= 6 and key.startswith(k))
                for n in v]
        nid = pick(sorted(set(pref)))
        if nid:
            return nid, "PROPOSED"
    return "", "GAP"
