"""Parse Galron-Goldschlager's Union List of Digitized Jewish Historic Newspapers
into one TSV.

Source: https://go.osu.edu/jpress  ->
  https://library.osu.edu/projects/hebrew-lexicon/Jewish-Press.htm

That page is an Excel-exported frameset carrying no rows of its own; the data
lives in two sheets, whose tab labels are the reverse of their file numbering:

  Jewish-Press_files/sheet001.htm  = Hebraic script (Hebrew/Yiddish/Ladino)
  Jewish-Press_files/sheet002.htm  = Latin + Cyrillic script

The live host 403s scripted requests, so the inputs here are saved copies
(from a browser, or from the Wayback Machine with a `<timestamp>id_/` prefix,
which serves the original file without the Archive's injected wrapper).

Both sheets are alphabetical, broken by single-cell letter-divider rows
("א", "0-9", "A"...). Those are dropped: a real row always carries years.
Each row's title cell hyperlinks to the scans; a title digitised in several
depositories repeats, one row per depository, so (title, depository, years)
is the unit here, not the title.
"""
from __future__ import annotations

import csv
import html
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

# (file, script label, whether the sheet's own column headers are Hebrew)
SHEETS = [
    ("jewish_press_hebraic.htm", "hebraic", True),
    ("jewish_press_latin.htm", "latin", False),
]

OUT = HERE / "galron_union_list.tsv"

COLUMNS = [
    "row_id", "script", "title", "title_latin", "title_hebrew",
    "place_of_publication", "years_covered", "language", "depository",
    "scan_url", "gated", "source_sheet",
]

TAG = re.compile(r"<[^>]+>")
ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
CELL = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
HREF = re.compile(r'href\s*=\s*"([^"]+)"', re.I)
HEBREW = re.compile(r"[֐-׿]")
LATIN_ALNUM = re.compile(r"[A-Za-z0-9]")

# Alphabet-divider rows carry a letter (or "0-9") in the title cell and nothing
# in the rest. Short titles are NOT a reliable signal on their own -- "הו" is a
# real Tel Aviv journal -- so the emptiness of the other cells decides.
DIVIDER = re.compile(r"^(?:0\s*[–-]\s*9|[A-Za-z]\s*[–-]\s*[A-Za-z]|"
                     r"[A-Za-z]|[֐-׿]{1,2})$")

# Excel's export opens each sheet with its own spreadsheet column letters
# ("א|ב|ג|ד|ה", "0-9|A|B|C|D"). Every cell is a bare letter, so the row has no
# place, no language and no link.
COLUMN_LETTERS = re.compile(r"^(?:[A-Za-z]|[֐-׿]|0\s*[–-]\s*9)$")

# A no-entry sign in the depository cell marks a subscription-gated repository.
GATE = "⛔"


def text(fragment: str) -> str:
    """Strip tags/entities and flatten the whitespace Excel's export injects."""
    plain = html.unescape(TAG.sub(" ", fragment)).replace("\xa0", " ")
    return re.sub(r"\s+", " ", plain).strip()


def parse_sheet(path: Path, script: str) -> list[dict]:
    raw = path.read_bytes()
    for enc in ("utf-8", "windows-1255", "windows-1252", "latin-1"):
        try:
            markup = raw.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise SystemExit(f"{path.name}: could not decode")

    out = []
    for chunk in ROW.findall(markup):
        raw_cells = CELL.findall(chunk)
        cells = [text(c) for c in raw_cells]
        if len(cells) < 5:
            continue

        title, place, years, language, depository = cells[:5]

        # Divider rows are a bare letter; the header row names its own columns.
        # A couple of real titles have an empty years cell, so absence of years
        # cannot itself disqualify a row -- only the divider shape can.
        if not title:
            continue
        if DIVIDER.match(title) and not (place or language):
            continue
        # Drop the column-letter header row: every cell a bare letter, no link.
        if all(COLUMN_LETTERS.match(c) for c in cells[:5] if c):
            continue
        if years.lower() in ("years covered", "שנים שנסרקו"):
            continue

        # The scans link hangs off whichever cell carries it (usually the title).
        scan_url = ""
        for cell in raw_cells:
            found = HREF.search(cell)
            if found and found.group(1).startswith("http"):
                scan_url = found.group(1)
                break

        gated = "Y" if GATE in depository else ""
        depository = depository.replace(GATE, "").strip()

        # Titles are frequently bilingual in one cell; split them so a matcher
        # can block on either script without re-deriving this every time.
        hebrew_part = " ".join(w for w in title.split() if HEBREW.search(w))
        latin_part = " ".join(w for w in title.split()
                              if not HEBREW.search(w) and LATIN_ALNUM.search(w))

        out.append({
            "script": script,
            "title": title,
            "title_latin": latin_part.strip(" =:-–"),
            "title_hebrew": hebrew_part.strip(" =:-–"),
            "place_of_publication": place,
            "years_covered": years,
            "language": language,
            "depository": depository,
            "scan_url": scan_url,
            "gated": gated,
            "source_sheet": path.name,
        })
    return out


def main() -> None:
    rows = []
    for filename, script, _hebrew_headers in SHEETS:
        path = HERE / filename
        if not path.exists():
            raise SystemExit(f"missing input: {path}")
        got = parse_sheet(path, script)
        print(f"{filename}: {len(got)} rows ({script})", file=sys.stderr)
        rows.extend(got)

    for i, row in enumerate(rows, start=1):
        row["row_id"] = f"galron-{i:04d}"

    with OUT.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=COLUMNS, delimiter="\t",
                                lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {OUT.relative_to(HERE.parent.parent)}: {len(rows)} rows",
          file=sys.stderr)


if __name__ == "__main__":
    main()
