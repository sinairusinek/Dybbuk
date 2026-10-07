"""Re-emit the mandala page's embedded song payload with the full song record.

The page currently carries {id, title, rec} per song and renders only `title`,
in a two-column list forced to RTL — which mangles the romanized entries. The
graph holds far more per song, so this widens the payload and the panel reads
it. Idempotent: run again after any entity_graph rebuild.
"""
import json
import re
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GRAPH = ROOT / "YiDraCor" / "data" / "entity_graph.json"
PAGE = ROOT / "docs" / "Visualizations" / "lateiner_hurwitz_mandala.html"

# Author strings as the songbooks print them, folded onto one name each. The
# raw value is kept on the node; this is display only. `Yozef Latayner [if
# 'Jüdale der Blinde']` is a conditional attribution, not a name — it keeps its
# hedge rather than being flattened to a bare credit.
AUTHOR_DISPLAY = {
    "yozef latayner": "Joseph Lateiner",
    "lateiner": "Joseph Lateiner",
    "hurwitz": "Moyshe Hurwitz",
    "goldfaden": "Avrom Goldfaden",
    "אברהם גולדפאדען": "Avrom Goldfaden",
    "גאלדפאדען": "Avrom Goldfaden",
    "שלמה שמולעוויטץ": "Sholem Shmulevitsh",
    "לואיס קאפעלמאן": "Louis Kapelman",
    "וואלף עהרענקראנץ": "Velvl Ehrenkrants",
    "סעם סמאלל": "Sam Small",
    "לואיס גילראד": "Louis Gilrod",
    "יצחק ריינגאלד": "Yitskhok Reyngold",
    "ד. בערנשטיין": "D. Bernshteyn",
    "m. rosenfeld": "Morris Rosenfeld",
    "m. zeifert": "Moyshe Zeifert",
    "feinman": "Zigmund Faynman",
    "anshel schor": "Anshel Schorr",
}

# Short labels for the 10 songbooks, so the panel can group by source without
# printing a 90-character title. The long form stays in the tooltip.
SOURCE_SHORT = {
    "יידיש ביהנע": "Di yidishe bihne",
    "די אידישע ביהנע : א זאמעלבוך פון ליעדער און דעקלאמאציאנען פיר גראמאפאנען":
        "Di yidishe bihne (zamelbukh)",
    "Shund on Shellac": "Shund on Shellac",
    "ליעדער מאגאזין No. II": "Lider magazin II",
    "לידער אונד קופלעטן פון די נייעסטע טהעאטער שטיקע "
    "(פערלאג פון די היברו פאבלישינג קאמפאני NYPL)": "Lider un kupletn (HPC)",
    "אללע ליעדער : פון פראפ. הורוויצעס נייעסטע מייסטערווערק "
    "דער משוגענער פילאזאף / פערפאסט פון פראפ. מ' איש הורוויץ הלוי":
        "Alle lider: Der meshugener filozof",
    "פערשיעדענע דעקלאמאציאנען אונד טהעאטער-ליעדער":
        "Fershiedene deklamatsyonen un teater-lieder",
    "דאס פידעלע: יודישע טהעאטער און פאלקס ליעדער (די היברו פאבלישינג קאמפאני, NYPL)":
        "Dos fidele (HPC)",
    "אללע ליעדער : פון דיא בעריהמטע אפערא דוד בן ישי":
        "Alle lider: Dovid ben Yishai",
    "Yankev und Eysev notebook from the Perlmutter collection":
        "Yankev un Eysev notebook (Perlmutter)",
}


def author_display(raw: str) -> str:
    if not raw:
        return ""
    key = raw.strip().lower()
    if key in AUTHOR_DISPLAY:
        return AUTHOR_DISPLAY[key]
    # Conditional attributions keep their condition.
    m = re.match(r"^(.*?)\s*(\[.*\])$", raw.strip())
    if m and m.group(1).strip().lower() in AUTHOR_DISPLAY:
        return f"{AUTHOR_DISPLAY[m.group(1).strip().lower()]} {m.group(2)}"
    return raw.strip()


def source_short(raw: str) -> str:
    raw = (raw or "").strip().rstrip(".")
    for k, v in SOURCE_SHORT.items():
        if k.rstrip(".") == raw:
            return v
    return raw


def num(v: str):
    """'6.0' -> 6. The sheet is read by openpyxl, so counts arrive as floats."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return int(f) if f == int(f) else f


def page_label(a: dict) -> str:
    """Printed page where the sheet gives one, else the PDF page, labelled."""
    p = num(a.get("page_printed"))
    if p is not None:
        return f"p. {p}"
    p = num(a.get("page_pdf"))
    return f"PDF p. {p}" if p is not None else ""


def has_hebrew(s: str) -> bool:
    return any("HEBREW" in unicodedata.name(c, "") for c in s)


def build_songs(graph) -> dict:
    """work_id -> [song payload], ordered by recordings then title."""
    works = {n["id"]: n for n in graph["nodes"] if n["kind"] == "work"}
    out: dict[str, list] = {}
    for n in graph["nodes"]:
        if n["kind"] != "song":
            continue
        wid = n.get("work_id")
        if not wid:
            continue

        # Two titles, each in its own script. The Yiddish column is populated
        # for all 225 rows; the roman one is whichever of the two romanizations
        # the sheet has, preferring YIVO over the source's German orthography.
        yid = (n.get("yiddish_title") or "").strip()
        label = (n.get("label") or "").strip()
        rom = label if label and not has_hebrew(label) else ""
        rom = rom or (n.get("romanized_title") or "").strip()
        src_sp = (n.get("title_in_source") or "").strip()
        if src_sp and src_sp.lower() == rom.lower():
            src_sp = ""

        song = {"id": n["id"]}
        if yid:
            song["yid"] = yid
        if rom:
            song["rom"] = rom
        if src_sp:
            song["src_title"] = src_sp

        # Lyricist, and whether it is someone other than the play's author —
        # a Goldfaden song in a Lateiner play is the interesting case.
        auth = author_display(n.get("author_raw", ""))
        if auth:
            song["by"] = auth
            pw = (works.get(wid, {}).get("playwright") or "").strip()
            if pw and pw.split()[-1].lower() not in auth.lower():
                song["other"] = True
        elif n.get("author_certainty") == "unknown":
            song["by_unknown"] = True

        attests = []
        for a in n.get("attestations", []):
            at = {"src": source_short(a.get("source_publication", ""))}
            full = (a.get("source_publication") or "").strip()
            if full and at["src"] != full:
                at["full"] = full
            pg = page_label(a)
            if pg:
                at["pg"] = pg
            if a.get("source_url"):
                at["url"] = a["source_url"]
            r = num(a.get("recordings"))
            if r:
                at["rec"] = r
            attests.append(at)
        if attests:
            song["att"] = attests

        r = num(n.get("recordings"))
        if r:
            song["rec"] = r
        if n.get("elaboration"):
            song["note"] = n["elaboration"]
        # 217 of 225 play-attachments are machine-proposed, not reviewed. The
        # panel says so rather than presenting all of them as settled.
        if n.get("link_status_to_work") == "reviewed":
            song["reviewed"] = True

        out.setdefault(wid, []).append(song)

    for wid, lst in out.items():
        lst.sort(key=lambda s: (-(s.get("rec") or 0),
                                (s.get("rom") or s.get("yid") or "").lower()))
    return out


def main():
    graph = json.loads(GRAPH.read_text(encoding="utf-8"))
    songs_by_work = build_songs(graph)

    html = PAGE.read_text(encoding="utf-8")
    marker = "window.__MANDALA__="
    i = html.index(marker) + len(marker)
    data, end = json.JSONDecoder().raw_decode(html[i:])

    # Idempotent: re-running over an already-enriched page rebuilds the same
    # payload from the graph, so it is safe after every entity_graph rebuild.
    before = sum(len(w.get("songs", [])) for w in data["works"])
    replaced = 0
    for w in data["works"]:
        new = songs_by_work.get(w["id"], [])
        if new or w.get("songs"):
            w["songs"] = new
            replaced += len(new)
    after = sum(len(w.get("songs", [])) for w in data["works"])
    assert before == after == 225, (before, after)

    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    PAGE.write_text(html[:i] + payload + html[i + end:], encoding="utf-8")
    print(f"songs rewritten: {after} across "
          f"{sum(1 for w in data['works'] if w['songs'])} works")
    fields = {}
    for w in data["works"]:
        for s in w["songs"]:
            for k in s:
                fields[k] = fields.get(k, 0) + 1
    print("fields:", dict(sorted(fields.items(), key=lambda x: -x[1])))


if __name__ == "__main__":
    main()
