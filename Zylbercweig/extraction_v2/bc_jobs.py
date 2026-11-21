"""Jobs B and C (v2 delta): sentence/window-level Gemini passes over mentions_all.

B: mentions with a blank or "Other" relation_category (6,565): classify the tie to the
   entry subject with the 11-15 scheme (+ Source, + None). Blank rows have no sentence,
   so the model gets text windows around the mention.
C: employer_director / employee_student relations whose sentence names a troupe,
   theatre, company or ensemble: is the tie mediated by an (eponymous) organization?

  GOOGLE_API_KEY=... python3.11 bc_jobs.py b [--limit N]
  GOOGLE_API_KEY=... python3.11 bc_jobs.py c [--limit N]
  python3.11 bc_jobs.py flatten        # jsonl -> b_recategorized.tsv, c_eponym_orgs.tsv
"""
import argparse, csv, json, re, sys, unicodedata
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import gemini_batch

SRC = HERE.parent / "people" / "mentions_all.tsv"
csv.field_size_limit(10**9)
ORG_WORD = re.compile(unicodedata.normalize(
    "NFC", r"(טרו?פּ?ע|טרופע|טעאַטער|טעאטער|קאָמפּאַני|ענסעמבל|תּיאַטער)"))


def windows(text, name, width=300, maxn=3):
    sur = name.split()[-1].strip(" .,") if name.split() else name
    out = []
    for m in re.finditer(re.escape(sur), text):
        a, b = max(0, m.start() - width), min(len(text), m.end() + width)
        out.append(text[a:b].replace("\n", " "))
        if len(out) >= maxn:
            break
    return out


ENTRY_TEXTS = HERE.parent / "people" / "entry_texts.tsv"


def load():
    """mentions_all fills host_xml_id / host_entry_text only on an entry's FIRST row;
    host_heading_fill is filled down. Join entry text by heading (38,096/38,269 match)
    and expose a per-row `entry_key` + `entry_text` + `entry_xml_id`."""
    rows = list(csv.DictReader(SRC.open(encoding="utf-8"), delimiter="\t"))
    by_head = {}
    for e in csv.DictReader(ENTRY_TEXTS.open(encoding="utf-8"), delimiter="\t"):
        by_head.setdefault(e["heading"].strip(), e)
    for r in rows:
        h = r["host_heading_fill"].strip()
        e = by_head.get(h)
        r["entry_key"] = r["src_file"] + "|" + h
        r["entry_text"] = e["entry_text"] if e else r["host_entry_text"]
        r["entry_xml_id"] = e["xml_id"] if e else r["host_xml_id"]
    return rows


def items_b(rows):
    out = []
    for r in rows:
        if r["relation_category"] not in ("", "Other"):
            continue
        if not r["mention_name"].strip():
            continue  # 5,931 fully empty artifact rows (no name/gender/sentence) -- not mentions
        out.append({"id": r["mention_id"], "entry_subject": r["host_heading_fill"],
                    "mention": r["mention_name"], "description": r["person_description"][:200],
                    "sentence": r["relation_sentence"][:400],
                    "windows": [] if r["relation_sentence"].strip() else
                    windows(r["entry_text"], r["mention_name"])})
    return out


def items_r(rows):
    """R (11-21): re-type the already-categorized rows to the 11-15 scheme (decision 4)."""
    out = []
    for r in rows:
        if r["relation_category"] in ("", "Other") or not r["mention_name"].strip():
            continue
        out.append({"id": r["mention_id"], "entry_subject": r["host_heading_fill"],
                    "mention": r["mention_name"], "description": r["person_description"][:200],
                    "sentence": r["relation_sentence"][:400],
                    "old": r["relation_category"] + "/" + r["relation_specific"]})
    return out


def items_c(rows):
    out = []
    for r in rows:
        if r["relation_specific"] not in ("employer_director", "employee_student"):
            continue
        s = unicodedata.normalize("NFC", r["relation_sentence"])
        if not ORG_WORD.search(s):
            continue
        out.append({"id": r["mention_id"], "entry_subject": r["host_heading_fill"],
                    "mention": r["mention_name"], "relation": r["relation_specific"],
                    "sentence": s[:500]})
    return out


R_TYPES = {  # r_retype.txt vocabulary: type -> category
    **{t: "Family" for t in ("parent", "child", "spouse", "sibling", "extended")},
    **{t: "Professional_Collaboration" for t in ("co_performer", "partner", "co_founder", "colleague")},
    **{t: "Professional_Hierarchy" for t in ("employer_director", "employee", "teacher", "student",
                                             "agent_impresario", "client")},
    **{t: "Creative_Intellectual" for t in ("creator", "interpreter", "translator_of", "adapter_of",
                                            "influence")},
    **{t: "Social_Personal" for t in ("friend", "acquaintance", "supporter_advocate", "conflict")},
    **{t: "Source" for t in ("informant_about", "wrote_about", "quoted_in_entry",
                             "corrected_or_contradicted")},
}
EXTENDED_SUB = {"in-law", "in_law", "cousin", "uncle_aunt", "grandparent", "ancestor"}


def normalize_r(rec):
    """Deterministic repair of off-vocabulary (category, type) pairs (QA 11-21: 119 rows).
    Family sub-relations given as type move to qualifier under `extended`; a valid type under
    the wrong category takes its own category. Every repair is noted in schema_issue."""
    cat, typ = rec.get("category", ""), rec.get("type", "")
    if cat == "None" or R_TYPES.get(typ) == cat:
        return rec
    rec = dict(rec)
    if cat == "Family" and typ in EXTENDED_SUB:
        rec["type"] = "extended"
        rec["qualifier"] = ", ".join(x for x in (typ, rec.get("qualifier", "")) if x)
    elif typ in R_TYPES:
        rec["category"] = R_TYPES[typ]
    elif cat == "Professional_Hierarchy" and typ == "director":
        rec["type"] = "employer_director"
    else:
        return rec  # left for reading; still off-vocabulary
    note = f"[norm {cat}/{typ}]"
    rec["schema_issue"] = " ".join(x for x in (note, rec.get("schema_issue", "")) if x)
    return rec


def flatten(rows):
    meta = {r["mention_id"]: r for r in rows}
    bcols = ["category", "type", "qualifier", "role_title", "via_work", "via_org", "evidence", "schema_issue"]
    for job, cols in (("b_recategorized", bcols), ("r_retyped", bcols),
                      ("c_eponym_orgs", ["org_mediated", "org_surface", "named_after_mention",
                                         "mention_role", "subject_role", "place", "date",
                                         "direct_tie_too", "evidence"])):
        src = HERE / f"{job}.jsonl"
        if not src.exists():
            continue
        recs = {}
        for line in src.open(encoding="utf-8"):
            rec = json.loads(line)
            if "error" not in rec and rec["id"] in meta and meta[rec["id"]]["mention_name"].strip():
                recs[rec["id"]] = normalize_r(rec) if job == "r_retyped" else rec
        head = ["mention_id", "host_xml_id", "host_heading", "mention_name",
                "old_category", "old_specific", "old_sentence"] + cols + ["model"]
        with (HERE / f"{job}.tsv").open("w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow(head)
            for mid, rec in recs.items():
                m = meta.get(mid, {})
                w.writerow([mid, m.get("entry_xml_id", ""), m.get("host_heading_fill", ""),
                            m.get("mention_name", ""), m.get("relation_category", ""),
                            m.get("relation_specific", ""), m.get("relation_sentence", "")[:300]]
                           + [str(rec.get(c, "")) for c in cols] + [rec.get("model", "")])
        print(f"{job}.tsv: {len(recs)} rows")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("job", choices=["b", "c", "r", "flatten", "count"])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--workers", type=int, default=12)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    rows = load()
    if a.job == "flatten":
        return flatten(rows)
    if a.job == "count":
        print("B", len(items_b(rows)), "C", len(items_c(rows)))
        return
    items = {"b": items_b, "c": items_c, "r": items_r}[a.job](rows)
    if a.offset:
        items = items[a.offset:]
    if a.limit:
        items = items[: a.limit]
    name = {"b": "b_recategorized", "c": "c_eponym_orgs", "r": "r_retyped"}[a.job]
    if a.out:
        name = a.out
    pf = {"b": "b_recategorize.txt", "c": "c_eponym_orgs.txt", "r": "r_retype.txt"}[a.job]
    prompt = (HERE / "prompts" / pf).read_text(encoding="utf-8")
    gemini_batch.run(items, prompt, HERE / f"{name}.jsonl", batch_size=25, workers=a.workers)


if __name__ == "__main__":
    main()
