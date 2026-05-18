"""Flatten ZylbercweigMentionedPeople20260228.tsv into mentions_all.tsv.

Source: Zylbercweig/ZylbercweigPeople/ZylbercweigMentionedPeople20260228.tsv
  ~38k rows — every in-text person mention across all 7 volumes, with rich
  context (gender, relation, place, dates).

IMPORTANT semantics:
  * The source column `PersonDBID` is the HOST ENTRY'S subject db_id
    (i.e. the DB id of the person whose entry contains this mention) — NOT
    the db_id of the person being mentioned. We expose it as
    `host_subject_db_id` to avoid confusion. There is no per-mention DB
    alignment ground truth in this file.
  * The source column `clustered names` groups mentions that refer to the
    same person across host entries. This is the unit of work for the
    mention→DB matcher.
  * `person?` = 'y' means the mention has been manually confirmed to be a
    person (vs. a typo / non-person token).
  * Some mentions are relational placeholders (`father*`, `mother*`, etc.) —
    each such mention refers to a different individual (the relative of
    whoever the host entry is about). These are flagged for a future
    relational-resolution pass and excluded from name-based matching.
"""
from __future__ import annotations
import csv, unicodedata
from collections import defaultdict, Counter
from pathlib import Path

csv.field_size_limit(10**8)

HERE = Path(__file__).parent
SRC = HERE.parent / "ZylbercweigPeople" / "ZylbercweigMentionedPeople20260228.tsv"
OUT_MENTIONS = HERE / "mentions_all.tsv"

# verbose column names → short ones we'll use everywhere
COL_MAP = {
    "_ - xml:id":                                   "host_xml_id",
    "ID-action":                                    "id_action",
    "PersonDBID":                                   "host_subject_db_id",  # host's subject, NOT mention's
    "person?":                                      "person_flag",
    "Archive":                                      "archive",
    "File":                                         "src_file",
    "_ - chunk_number":                             "chunk_number",
    "EntryType":                                    "entry_type",
    "entrytypereviewed":                            "entry_type_reviewed",
    "_ - heading":                                  "host_heading",
    "headingfilldown":                              "host_heading_fill",
    "_ - span":                                     "host_span",
    "_ - entry":                                    "host_entry_text",
    "_ - credit":                                   "host_credit",
    "_ - subheading":                               "host_subheading",
    "_ - people - _ - name":                        "mention_name",
    "clustered names":                              "cluster_name",
    "towd":                                         "towd",
    "extracted names issue":                        "extraction_issue",
    "_ - people - _ - gender":                      "gender",
    "_ - people - _ - person_description":          "person_description",
    "_ - people - _ - relationship - category":     "relation_category",
    "_ - people - _ - relationship - specific_relation":  "relation_specific",
    "_ - people - _ - relationship - original_sentence":  "relation_sentence",
    "_ - people - _ - relationship - place - venue":      "rel_place_venue",
    "_ - people - _ - relationship - place - settlement": "rel_place_settlement",
    "_ - people - _ - relationship - place - country":    "rel_place_country",
    "_ - people - _ - relationship - place - province":   "rel_place_province",
    "_ - people - _ - relationship - place - address":    "rel_place_address",
    "_ - people - _ - relationship - date_start - date":  "rel_date_start",
    "_ - people - _ - relationship - date_end - date":    "rel_date_end",
    "_ - endnotes - endnotes":                      "host_endnotes",
}


def _norm(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() == "none":
        return ""
    return s.replace("\t", " ").replace("\n", " ").replace("\r", " ")


# Relational placeholder mentions — names like "father*", "mother*", "wife*" etc.
# These are NOT real persons by name; each row refers to a different individual
# (the host entry's subject's relative). They must be excluded from name-based
# matching but kept for a future relational-resolution pass that uses the host
# entry's subject + the relation type.
RELATIONAL_STOPNAMES_YI = {
    "פֿאָטער", "פאטער", "פֿאטער",   # father
    "מוּטער", "מוטער",              # mother
    "עלטערן",                       # parents
    "פרוי", "פֿרוי",                # wife
    "מאן", "מאַן",                   # husband
    "בּרוּדער", "ברודער", "בּרודער",  # brother
    "שוועסטער",                     # sister
    "זון",                          # son
    "טאָכטער", "טאכטער",            # daughter
    "זיידע", "זייידע",               # grandfather
    "באבע", "באָבע", "באבא",         # grandmother
    "פעטער", "פֿעטער",              # uncle
    "מומע", "מוּמע",                 # aunt
    "אייניקל",                      # grandchild
    "פֿאַמיליע", "פאמיליע",         # family
    "קינדער",                       # children
    "*",                            # bare placeholder
}


def _strip_marks(s: str) -> str:
    """NFKC-normalize (collapses Hebrew presentation-form ligatures like U+FB4E
    'פֿ' into decomposed פ+ׂ), then strip asterisks + spaces."""
    return unicodedata.normalize("NFKC", s).replace("*", "").replace(" ", "").strip()


def is_relational_placeholder(cluster_name: str, mention_name: str) -> bool:
    """True iff this mention is a relational placeholder (father*, mother*, etc.)
    rather than a real named person."""
    stopnames_nfkc = {unicodedata.normalize("NFKC", s) for s in RELATIONAL_STOPNAMES_YI}
    for s in (cluster_name, mention_name):
        if not s:
            continue
        if s.endswith("*"):
            stripped = _strip_marks(s)
            if not stripped or stripped in stopnames_nfkc:
                return True
        elif _strip_marks(s) in stopnames_nfkc:
            return True
    return False


def main():
    rows = []
    with open(SRC, encoding="utf-8") as fp:
        reader = csv.DictReader(fp, delimiter="\t")
        for r in reader:
            out = {short: _norm(r.get(orig, "")) for orig, short in COL_MAP.items()}
            rows.append(out)

    # Stable mention_id: idx within (host_xml_id, mention_name)
    by_xid_name: dict[tuple[str, str], int] = defaultdict(int)
    for r in rows:
        key = (r["host_xml_id"], r["mention_name"])
        idx = by_xid_name[key]
        r["mention_id"] = f"M-{r['src_file'].replace('.json','')}-{r['host_xml_id'] or 'NOXID'}-{key[1][:30]}-{idx:03d}"
        by_xid_name[key] += 1

    # Tag relational placeholders
    for r in rows:
        r["relational_placeholder"] = "1" if is_relational_placeholder(
            r["cluster_name"], r["mention_name"]
        ) else ""

    out_fields = ["mention_id"] + list(COL_MAP.values()) + ["relational_placeholder"]
    with open(OUT_MENTIONS, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=out_fields, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {OUT_MENTIONS} ({len(rows)} rows)")

    # Stats only — no fake "alignment" claims.
    n_person_y = sum(1 for r in rows if r["person_flag"].lower() == "y")
    n_clustered = sum(1 for r in rows if r["cluster_name"])
    n_placeholder = sum(1 for r in rows if r["relational_placeholder"])
    n_host_db = sum(1 for r in rows if r["host_subject_db_id"])
    distinct_clusters = len({r["cluster_name"] for r in rows if r["cluster_name"]})

    print(f"\nMention universe summary:")
    print(f"  total rows:                          {len(rows)}")
    print(f"  rows with a mention name:            {sum(1 for r in rows if r['mention_name'])}")
    print(f"  rows flagged person?='y':            {n_person_y}")
    print(f"  rows in a 'clustered names' cluster: {n_clustered}  ({distinct_clusters} distinct clusters)")
    print(f"  rows tagged relational_placeholder:  {n_placeholder}")
    print(f"  rows with host_subject_db_id:        {n_host_db}  (host context, NOT mention alignment)")
    print(f"\nNo per-mention DB-id ground truth exists in this file. The mention→DB")
    print(f"alignment pipeline must generate alignments from scratch.")


if __name__ == "__main__":
    main()
