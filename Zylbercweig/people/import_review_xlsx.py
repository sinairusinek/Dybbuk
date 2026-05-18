"""Convert ZylbercweigPeople xlsx review artifacts into TSVs.

Outputs (next to this script):
  - people_db.tsv               — extracted "DiJeSt DB" / Zylbercweig-people target rows
                                  (DB-ID, hebrew_name, english_name, source_url_fragment)
  - people_alignment_review.tsv — RA alignment + duplication decisions per subject-entry
                                  (xml_id, heading, volume, DB-ID, same_person, action)
  - people_index_unmatched.tsv  — index names not matched to any entry heading
                                  ("לא מזוהים מהאינדקס" columns, per volume)
  - mention_validations_full.tsv     — full mention -> heading mapping w/ occurrence counts
  - mention_validations_initials.tsv — initial-form -> heading (e.g. "ב. גאָרין")
  - mention_validations_surnames.tsv — bare surname -> heading w/ disambiguation
  - credited_persons.tsv             — byline credits ("מ. ע. פֿון X") + where they appear

These are read-only conversions. They form the ground-truth pool for the
people-matcher drafter + holdout tests (analogous to Tests 1/2 for orgs).
"""
from __future__ import annotations
import csv, re
from pathlib import Path
import openpyxl

HERE = Path(__file__).parent
PEOPLE_XLSX = HERE.parent / "ZylbercweigPeople"
JSON_REVIEW = PEOPLE_XLSX / "Json Review (8).xlsx"
CREDITED = PEOPLE_XLSX / "credited persons.xlsx"
NAME_VALID = PEOPLE_XLSX / "extracted names validations.xlsx"

DB_RE = re.compile(r"^\s*(\d+)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*$")


def _norm(v):
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() == "none":
        return ""
    return s.replace("\t", " ").replace("\n", " ")


def parse_db_keys(json_review_path: Path):
    """Pull every '{id}|{hebname}|{english}' record we can find across sheets,
    plus the keyFromDB sheet which is the cleanest source. Returns dict id->row.
    """
    wb = openpyxl.load_workbook(json_review_path, read_only=True, data_only=True)
    db = {}
    # keyFromDB sheet is the canonical list
    if "keyFromDB" in wb.sheetnames:
        ws = wb["keyFromDB"]
        for row in ws.iter_rows(min_row=2, values_only=True):
            key = _norm(row[0] if row else "")
            if not key:
                continue
            # format varies: '1255|Tsvi Ben-Menachem' (id|english) or '1255|heb|english'
            parts = [p.strip() for p in key.split("|")]
            if len(parts) >= 2 and parts[0].isdigit():
                dbid = parts[0]
                if len(parts) == 2:
                    db.setdefault(dbid, {"db_id": dbid, "hebname": "", "english": parts[1], "raw": key})
                else:
                    db.setdefault(dbid, {"db_id": dbid, "hebname": parts[1], "english": parts[2] if len(parts) > 2 else "", "raw": key})
    # also harvest DB-ID columns from per-volume sheets (they include richer hebname+english)
    for sname in wb.sheetnames:
        ws = wb[sname]
        header = None
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [_norm(c).lower() for c in row]
                continue
            if not header:
                break
            for col_idx, h in enumerate(header):
                if h in ("db-id", "dbid"):
                    val = _norm(row[col_idx] if col_idx < len(row) else "")
                    m = DB_RE.match(val)
                    if m:
                        dbid, heb, eng = m.group(1), m.group(2), m.group(3)
                        existing = db.get(dbid)
                        if not existing or not existing.get("hebname"):
                            db[dbid] = {"db_id": dbid, "hebname": heb, "english": eng, "raw": val}
    wb.close()
    return db


def parse_alignment_review(json_review_path: Path):
    """Walk per-volume sheets + Duplication Check + alignment sheet, emit per-row
    RA decisions tied to xml_id. We don't try to interpret every column — we keep
    the fields most likely to be ground-truth signal.
    """
    wb = openpyxl.load_workbook(json_review_path, read_only=True, data_only=True)
    rows = []
    # Duplication Check
    if "Duplication Check" in wb.sheetnames:
        ws = wb["Duplication Check"]
        header = None
        for i, r in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [_norm(c) for c in r]
                continue
            d = {h: _norm(v) for h, v in zip(header, r)}
            if not d.get("_ - xml:id"):
                continue
            rows.append({
                "source_sheet": "Duplication Check",
                "volume": d.get("vol", ""),
                "xml_id": d.get("_ - xml:id", ""),
                "chunk_number": d.get("_ - chunk_number", ""),
                "heading": d.get("_ - heading", ""),
                "task": d.get("task", ""),
                "action": d.get("action", ""),
                "same_person": d.get("Same person?", ""),
                "db_id_raw": d.get("DB-ID", ""),
                "duplication_study": d.get("duplication study", ""),
                "notes": d.get("הערות", ""),
                "additional_pages": d.get("additional pages", ""),
                "additional_volume": d.get("additional volume", ""),
                "decision": "",  # not present in this sheet
            })
    # main alignment sheet
    if "alignment" in wb.sheetnames:
        ws = wb["alignment"]
        header = None
        for i, r in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [_norm(c) for c in r]
                continue
            d = {h: _norm(v) for h, v in zip(header, r)}
            if not d.get("_ - xml:id"):
                continue
            rows.append({
                "source_sheet": "alignment",
                "volume": d.get("vol", ""),
                "xml_id": d.get("_ - xml:id", ""),
                "chunk_number": d.get("_ - chunk_number", ""),
                "heading": d.get("_ - heading", ""),
                "task": "",
                "action": d.get("action", ""),
                "same_person": "",
                "db_id_raw": d.get("DB-ID", ""),
                "duplication_study": "",
                "notes": d.get("הערות", ""),
                "additional_pages": d.get("additional pages", ""),
                "additional_volume": d.get("additional volume", ""),
                "decision": d.get("Column", ""),  # used in this sheet for role/cat
            })
    wb.close()
    # parse db_id_raw into a numeric id if possible
    for row in rows:
        raw = row["db_id_raw"]
        m = DB_RE.match(raw) if raw else None
        if m:
            row["db_id"] = m.group(1)
            row["db_hebname"] = m.group(2)
            row["db_english"] = m.group(3)
        else:
            # sometimes raw is just a float-looking id
            row["db_id"] = raw.split(".")[0] if raw and raw.replace(".", "").isdigit() else ""
            row["db_hebname"] = ""
            row["db_english"] = ""
    return rows


def parse_index_unmatched(json_review_path: Path):
    """Per-volume 'align' sheets list index names that didn't match an entry heading."""
    wb = openpyxl.load_workbook(json_review_path, read_only=True, data_only=True)
    rows = []
    align_sheets = [s for s in wb.sheetnames if s.lower().endswith("align") or "align" in s.lower() and s != "alignment"]
    for sname in align_sheets:
        ws = wb[sname]
        header = None
        for i, r in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [_norm(c) for c in r]
                continue
            d = {h: _norm(v) for h, v in zip(header, r)}
            # the "unmatched" payload sits in an explicit column or trailing columns
            unmatched = d.get("לא מזוהים מהאינדקס", "")
            heading = d.get("_ - heading", "")
            vol = d.get("vol", "")
            xml_id = d.get("_ - xml:id", "")
            # also pick up any non-empty 'Column...' values which are extra index names
            extras = []
            for k, v in d.items():
                if v and (k.startswith("Column") or k == "Column 1 2 in Zylbercweig people"):
                    extras.append(v)
            if not (unmatched or heading or extras):
                continue
            rows.append({
                "source_sheet": sname,
                "volume": vol,
                "xml_id": xml_id,
                "heading": heading,
                "index_unmatched": unmatched,
                "extras": " || ".join(extras),
            })
    wb.close()
    return rows


def parse_name_validations(path: Path):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    out = {"full": [], "initials": [], "surnames": []}

    def harvest(sheet_name, key_col, freq_col, heading_col, alt_col=None, notes_col=None):
        if sheet_name not in wb.sheetnames:
            return []
        ws = wb[sheet_name]
        header = None
        rows = []
        for i, r in enumerate(ws.iter_rows(values_only=True)):
            if i == 0:
                header = [_norm(c) for c in r]
                continue
            d = {h: _norm(v) for h, v in zip(header, r)}
            mn = d.get(key_col, "")
            if not mn:
                continue
            rows.append({
                "mention": mn,
                "occurrences": d.get(freq_col, ""),
                "as_heading": d.get(heading_col, ""),
                "alternative_heading": d.get(alt_col, "") if alt_col else "",
                "notes": d.get(notes_col, "") if notes_col else "",
            })
        return rows

    out["full"] = harvest("validate extracted names", "mentioned name", "occurances as mentioned", "as entry heading", notes_col="הערות")
    out["initials"] = harvest("initials", "initials", "occurences", "as heading", alt_col="alternative heading", notes_col="הערות")
    out["surnames"] = harvest("surnames only", "Single-word", "occurences", "as heading", alt_col="alternative heading for disambiguation", notes_col="הערות ושאלות")
    wb.close()
    return out


def parse_credited(path: Path):
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb["Sheet1"]
    header = None
    rows = []
    for i, r in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            header = [_norm(c) for c in r]
            continue
        d = {h: _norm(v) for h, v in zip(header, r)}
        if not d.get("Credit"):
            continue
        rows.append({
            "credit": d.get("Credit", ""),
            "occurrences": d.get("where to find it", ""),
            "clustered": d.get("clustered", ""),
            "full_name": d.get("add full name here", ""),
            "comments": d.get("comments if there are any", ""),
            "qid": d.get("Qid", ""),
        })
    wb.close()
    return rows


def write_tsv(path: Path, rows, fieldnames):
    with open(path, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {path} ({len(rows)} rows)")


def main():
    db = parse_db_keys(JSON_REVIEW)
    write_tsv(HERE / "people_db.tsv", list(db.values()),
              ["db_id", "hebname", "english", "raw"])

    align = parse_alignment_review(JSON_REVIEW)
    write_tsv(HERE / "people_alignment_review.tsv", align,
              ["source_sheet", "volume", "xml_id", "chunk_number", "heading",
               "task", "action", "same_person", "decision",
               "db_id", "db_hebname", "db_english", "db_id_raw",
               "duplication_study", "notes", "additional_pages", "additional_volume"])

    unmatched = parse_index_unmatched(JSON_REVIEW)
    write_tsv(HERE / "people_index_unmatched.tsv", unmatched,
              ["source_sheet", "volume", "xml_id", "heading", "index_unmatched", "extras"])

    nv = parse_name_validations(NAME_VALID)
    write_tsv(HERE / "mention_validations_full.tsv", nv["full"],
              ["mention", "occurrences", "as_heading", "alternative_heading", "notes"])
    write_tsv(HERE / "mention_validations_initials.tsv", nv["initials"],
              ["mention", "occurrences", "as_heading", "alternative_heading", "notes"])
    write_tsv(HERE / "mention_validations_surnames.tsv", nv["surnames"],
              ["mention", "occurrences", "as_heading", "alternative_heading", "notes"])

    credited = parse_credited(CREDITED)
    write_tsv(HERE / "credited_persons.tsv", credited,
              ["credit", "occurrences", "clustered", "full_name", "comments", "qid"])


if __name__ == "__main__":
    main()
