"""Build the troupe-tagging spreadsheet for Ruthie (replaces the Zalmen tagger).

Row 1 = the closed tag vocabulary (16 tags, " | "-separated); row 2 = headings.
Universe = every troupe in troupe_tags_draft.tsv ∪ troupe_tags.tsv.
Re-mirror troupe_tags.tsv / troupe_tag_review.tsv from the zalmen-data branch
before running (see zalmen/troupe_store.py docstring).

    python3 Zylbercweig/organizations/build_troupe_tag_sheet.py
"""
from __future__ import annotations
import csv, pathlib, sys, collections
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "zalmen"))
csv.field_size_limit(10**9)

ORG = pathlib.Path(__file__).resolve().parent
OUT = ORG / "troupe_tags_sheet.csv"
SEP = " | "

TAG_OPTS = [
    "Family Company", "Impresario Company", "Star Company", "Ensemble Company",
    "Cooperative Company", "Institutional Company", "Ad Hoc Company",
    "Children's Company", "Operetta / Opera Company", "German-Jewish Company",
    "Amateur Company", "Kleinkunst / Revue / Cabaret Company",
    "Marionette / Puppet Company", "Non-Jewish Company",
    "Hebrew-Language Company", "Not a Troupe",
]

def rows(p): return list(csv.DictReader(open(p, newline="", encoding="utf-8-sig"), delimiter="\t"))
def split(s): return [x.strip() for x in (s or "").split("|") if x.strip()]

core   = {r["db_id"]: r for r in rows(ORG / "core_db.tsv")}
drafts = {r["db_id"]: r for r in rows(ORG / "troupe_tags_draft.tsv")}
tags   = {r["db_id"]: r for r in rows(ORG / "troupe_tags.tsv")}
review = {r["db_id"]: r for r in rows(ORG / "troupe_tag_review.tsv")}

# cluster_id → mention strings + (heading, sentence) lines, minus removed mentions
try:
    import mention_removals
    removed = mention_removals.load_removed_keys()
    mkey = mention_removals.mention_key
except Exception as e:  # streamlit-less fallback
    print("WARN: mention_removals unavailable:", e); removed, mkey = set(), None
mentions = collections.defaultdict(list); texts = collections.defaultdict(list)
seen_m, seen_t = set(), set()
with open(ORG / "organizations_clustered.tsv", newline="", encoding="utf-8-sig") as f:
    for r in csv.DictReader(f, delimiter="\t"):
        cid = r.get("cluster_id", "").strip()
        if not cid: continue
        if removed and mkey and mkey(r) in removed: continue
        m = r.get("clustered organization", "").strip()
        if m and (cid, m) not in seen_m: seen_m.add((cid, m)); mentions[cid].append(m)
        head = r.get("_ - heading", "").strip()
        sent = r.get("_ - organizations - _ - relations - _ - original_sentence", "").strip()
        if sent and (cid, head, sent) not in seen_t:
            seen_t.add((cid, head, sent)); texts[cid].append(f"[{head}] {sent}" if head else sent)

def clusters(db):
    d = drafts.get(db, {}); c = core.get(db, {})
    return split(d.get("cluster_ids")) or split(c.get("linked_cluster_ids"))

headers = ["db_id", "name", "name_yiddish", "org_type", "entry_text", "mentions",
           "tags", "draft_tags", "status", "comment", "reviewer", "reviewed_at"]
universe = sorted(set(drafts) | set(tags), key=int)
out = []
for db in universe:
    c = core.get(db, {}); d = drafts.get(db, {}); t = tags.get(db, {})
    cids = clusters(db)
    clean = lambda s: s.replace("\n", " ").replace("\r", " ")
    reviewed = db in tags or db in review
    out.append({
        "db_id": db,
        "name": c.get("name") or d.get("name", ""),
        "name_yiddish": c.get("name_yiddish") or d.get("name_yiddish", ""),
        "org_type": c.get("org_type", ""),
        "entry_text": SEP.join(clean(x) for cid in cids for x in texts.get(cid, [])),
        "mentions": SEP.join(dict.fromkeys(m for cid in cids for m in mentions.get(cid, []))),
        "tags": SEP.join(split(t.get("tags"))) if t else "",
        "draft_tags": SEP.join(split(d.get("tags"))) if d else "",
        "status": "reviewed" if reviewed else "not reviewed",
        "comment": t.get("comment", ""),
        "reviewer": t.get("reviewer", ""),
        "reviewed_at": t.get("reviewed_at", ""),
    })

with open(OUT, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["OPTIONAL TAGS:", SEP.join(TAG_OPTS)])
    w.writerow(headers)
    for r in out: w.writerow([r[h] for h in headers])
n_rev = sum(r["status"] == "reviewed" for r in out)
n_txt = sum(bool(r["entry_text"]) for r in out)
print(f"{OUT.name}: {len(out)} troupes, {n_rev} reviewed, {n_txt} with entry text")
