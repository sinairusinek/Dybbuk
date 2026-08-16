"""Draft troupe tags from the lexicon, using Ruthie's demonstrated rules.

Reads evidence gathered per troupe (name + mention sentences) and applies the
deduction rules ratified with Ruthie (see TROUPE_TAGGING_RULES.md), each tag
citing the Yiddish phrase or the known-name that triggered it. Curated
family/star/operetta knowledge is applied conservatively and marked as such so
the reviewer can weigh it separately from hard text cues.

Live vocabulary only (the flat app list), plus the two new tags Ruthie approved:
  Kleinkunst / Revue / Cabaret Company, Marionette / Puppet Company.
Youth/student folds into Amateur (her decision). Not adopted: Wandering,
Provincial, Drama, Art-Theatre, Hebrew, Women-Led, Guest-Star.

Output: troupe_tags_draft.tsv — reviewed later in Zalmen (separate tab, TBD).
NOT written into troupe_tags.tsv; these are drafts, source=Claude-draft.
"""
from __future__ import annotations
import csv, json, re, pathlib

csv.field_size_limit(10**9)
HERE = pathlib.Path(__file__).resolve().parent
TROUPE_TYPES = {"traveling company", "company on tour"}
SENT_COL = "_ - organizations - _ - relations - _ - original_sentence"


def _load(p):
    with open(HERE / p, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def _is_live(r: dict) -> bool:
    """Skip rows that no longer denote a taggable entity: merged away,
    deprecated, or explicitly out of project scope."""
    return not any((r.get(k) or "").strip()
                   for k in ("deprecated", "merged_into", "out_of_project"))


def gather_evidence() -> dict[str, dict]:
    """Per troupe DB: name, Yiddish, its cluster ids, and every mention
    sentence for those clusters. One pass over organizations_clustered.tsv.

    The troupe LIST comes from core_db, not db_audit_punchlist: the punchlist
    only holds DBs flagged for false-equation review, which is 152 of the 699
    traveling companies. Drafting off it silently capped tagging at ~22% of the
    corpus.

    Cluster ids are the UNION of core_db.linked_cluster_ids and the punchlist's
    cluster_details_json. Neither source alone is complete: for 122 troupes the
    punchlist (2026-08-10) still records clusters that core_db no longer links,
    with no REMOVE in db_audit_decisions.tsv to account for it — and those
    clusters do still carry mentions in organizations_clustered.tsv (e.g. DB 433
    loses both of its clusters, one with 46 mentions, and would otherwise be
    drafted from its name alone). Taking the union keeps that evidence while
    leaving coverage driven by core_db. The dropped links look like a real
    integrity bug and are worth chasing separately; do not read this union as a
    fix for it.
    """
    troupes = [r for r in _load("core_db.tsv")
               if (r.get("org_type") or "").strip().lower() in TROUPE_TYPES
               and _is_live(r)]
    pl_cids: dict[str, list[str]] = {}
    for r in _load("db_audit_punchlist.tsv"):
        pl_cids[r["db_id"]] = [c["cluster_id"] for c in
                               json.loads(r.get("cluster_details_json", "[]") or "[]")]

    def _cids_for(r: dict) -> list[str]:
        seen, out = set(), []
        for c in ([x.strip() for x in (r.get("linked_cluster_ids") or "").split("|")]
                  + pl_cids.get(r["db_id"], [])):
            if c and c not in seen:
                seen.add(c)
                out.append(c)
        return out

    # One pass over the clustered file: per-cluster mention sentences, and a
    # canonical-Yiddish → cluster_id map (first occurrence) used to rescue
    # troupes that core_db never linked. The file is small (~16k rows), so
    # holding every cluster's sentences in memory is cheap.
    cid_sents: dict[str, list[str]] = {}
    canon2cid: dict[str, str] = {}
    with open(HERE / "organizations_clustered.tsv", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f, delimiter="\t")
        cidcol = next((c for c in rd.fieldnames if c.strip().lower() == "cluster_id"), None)
        for row in rd:
            cid = (row.get(cidcol) or "").strip()
            if not cid:
                continue
            cy = (row.get("canonical_yiddish") or "").strip()
            if cy and cy not in canon2cid:
                canon2cid[cy] = cid
            s = (row.get(SENT_COL) or "").strip()
            if s:
                cid_sents.setdefault(cid, []).append(s)

    out = {}
    for r in troupes:
        db = r["db_id"]
        cids = _cids_for(r)
        sents = [s for c in cids for s in cid_sents.get(c, [])]
        # text_source: linked (mentions came from core_db/punchlist links),
        # name-match (rescued by exact canonical-Yiddish name — NOT a confirmed
        # link, just recovered evidence), or none (genuinely no lexicon text).
        source = "linked" if sents else "none"
        if not sents:
            mcid = canon2cid.get((r.get("name_yiddish") or "").strip())
            if mcid and cid_sents.get(mcid):
                if mcid not in cids:
                    cids = cids + [mcid]
                sents = cid_sents[mcid]
                source = "name-match"
        out[db] = {
            "name": r.get("name", ""),
            "yiddish": r.get("name_yiddish", ""),
            "n_clusters": str(len(cids)),
            "cids": cids,
            "sents": sents,
            "text_source": source,
        }
    return out


EV = gather_evidence()

# ── curated knowledge (pruned to Ruthie-confirmed cases; Yiddish substrings) ──
# Kept deliberately small: better to under-tag Star/Family (she adds them) than
# assert a knowledge tag her scheme disagrees with (she treats Kaminska as
# Family, not Star; Adler as Star, not Family).
FAMILIES = {  # → Family Company
    "קאַמינסק":"Kaminski", "קאמינסק":"Kaminski", "טורקאָוו":"Turkow",
    "קאָריק":"Korik", "קאַריק":"Korik",
}
STARS = {  # → Star Company (famous performers, troupe is a star-vehicle)
    "אַדלער":"Adler", "אָדלער":"Adler", "טאָמאַשעווסקי":"Thomashefsky",
    "שוואַרץ":"Schwartz", "סידי טאָל":"Sidi Tal", "זאַסלאַווסקי":"Zaslavsky",
    "זאָסלאָווסקי":"Zaslavsky", "זאַסלאווסקי":"Zaslavsky", "טורקאָוו":"Turkow",
    "פישזאָן":"Fishzon", "פֿישזאָן":"Fishzon", "סאַץ":"Satz",
    "לעבעדעוו":"Lebedeff", "פּיקאָן":"Picon",
}
# Operetta only from the eponym IN THE NAME (never from career mentions).
OPERETTA_NAMES = {
    "הורוויץ":"Hurwitz", "הוּרוויץ":"Hurwitz", "שמ'ר":"Shomer", "שמיר":"Shomer",
    "שאָמער":"Shomer", "שייקעוויטש":"Shomer",
    "קדיש-כאַש":"Kadish-Khash", "קאַדיש-כאַש":"Kadish-Khash", "כאַש-קדיש":"Kadish-Khash",
}
# Yiddish compound suffixes that a hyphen can attach — NOT a second surname.
_HYPHEN_NOT_NAME = ("טרופּ","טרופ","קרייז","קאָלעקטיוו","קוואַרטעט","אָפּערעט","קינדער",
                    "וואַנדער","פּראָווינץ","טעאַטער","אַנסאַמבל","יידיש","געזעלשאַפֿט")

def cue(text, pats):
    for p in pats:
        m = re.search(p, text)
        if m: return m.group(0)
    return None

def classify(name, yid, sents):
    hay = " ".join([name, yid] + sents)
    nm  = name + " " + yid
    tags, ev, flags = [], [], []
    def add(tag, why, conf):
        if tag not in tags:
            tags.append(tag); ev.append(f"{tag} ⟵ {why} [{conf}]")

    # ── hard text cues, from the NAME unless noted (high confidence) ──
    # Ad Hoc — two billed names joined in the NAME (Latin "and"/hyphen, or
    # Yiddish "X און Y", or surname-surname hyphen that is not a compound word).
    ah = cue(nm, [r"[A-Z]\w+ and [A-Z]\w+", r"[A-Z]\w+-[A-Z]\w+", r"\S+ און \S+"])
    if not ah:
        m = re.search(r"([א-ת]{3,})-([א-ת]{3,})", nm)
        if m and not any(m.group(2).startswith(x) for x in _HYPHEN_NOT_NAME):
            ah = m.group(0)
    if ah: add("Ad Hoc Company", f"joined names «{ah}»", "text-low")
    # Children's — in the name
    c = cue(nm, [r"קינדער", r"[Cc]hildren"])
    if c: add("Children's Company", f"«{c}»", "text")
    # Kleinkunst / Revue / Cabaret — name or mentions
    k = cue(hay, [r"קליינקונסט", r"רעוויו", r"קאַבאַרעט", r"קוואַרטעט", r"מיניאַטור"])
    if k: add("Kleinkunst / Revue / Cabaret Company", f"«{k}»", "text")
    # Marionette / Puppet
    mar = cue(hay, [r"מאַריאָנעט", r"ליאַלק", r"[Mm]arionette", r"[Pp]uppet"])
    if mar: add("Marionette / Puppet Company", f"«{mar}»", "text")
    # Operetta — from the NAME only (never career mentions), or known eponym in name
    op = cue(nm, [r"אָפּערעט", r"אָפּערע\b", r"[Oo]peret"])
    if op: add("Operetta / Opera Company", f"«{op}»", "text")
    else:
        for sub, who in OPERETTA_NAMES.items():
            if sub in nm: add("Operetta / Opera Company", f"operetta director: {who}", "knowledge"); break
    # Cooperative
    co = cue(hay, [r"קאָאָפּעראַטיוו", r"אַרטעל", r"קאָלעקטיוו"])
    if co: add("Cooperative Company", f"«{co}»", "text")
    # Institutional — strong parent-body cues, or the name IS an acronym
    inst = cue(hay, [r"קולטור-?ליגע", r"פֿאַראיין", r"יוניאָן", r"פּראָלעט",
                     r"אַרבעטער-רינג", r"לאַנדסמאַנשאַפֿט"])
    if not inst and re.search(r'[א-ת]"[א-ת]', name.strip() or "\x00"):
        inst = name.strip()  # e.g. פֿאָד"א — an abbreviation-name = an institution
    if inst: add("Institutional Company", f"«{inst}»", "text")
    # Amateur (incl. youth/student, per Ruthie) — NAME only (a mention that an
    # actor "started as an amateur" is career history, not this troupe's type).
    am = cue(nm, [r"אַמאַטאָר", r"ליבהאָבער", r"דראַמאַטיש\w*[- ]קרייז",
                  r"דראַמ[- ]קרייז", r"יוגנט[- ]", r"סטודענטן?[- ]", r"שול[- ]טרופּ"])
    if am: add("Amateur Company", f"«{am}»", "text")
    # (Zionist / Socialist cues removed 2026-08-10: Ruthie dropped those tags,
    # along with Post-Holocaust and Bilingual, as unconfirmed and unused.)
    # Family — brothers/sisters/family word in the NAME (a mention of "the
    # brothers" elsewhere is about a person's career, not this troupe).
    fam = cue(nm, [r"ברידער", r"שוועסטער", r"משפּחה", r"[Ff]amily"])
    if fam: add("Family Company", f"«{fam}»", "text")

    # ── curated knowledge (medium confidence), name-only ──
    for sub, who in FAMILIES.items():
        if sub in nm: add("Family Company", f"known family: {who}", "knowledge"); break
    for sub, who in STARS.items():
        if sub in nm: add("Star Company", f"known star: {who}", "knowledge"); break

    # ── German-Jewish: flag only, never assert (Ruthie's call needed) ──
    gj = cue(hay, [r"\bווין\b", r"\bבערלין\b", r"דײַטשלאַנד", r"דייטשלאַנד"])
    if gj:
        flags.append(f"maybe German-Jewish? (tours «{gj}») — needs Ruthie")

    # ── Impresario base — person-named touring troupe, unless institutional/coop ──
    institutional = any(t in tags for t in ("Institutional Company", "Cooperative Company"))
    if not institutional:
        add("Impresario Company", "base: eponymous manager-led troupe", "base")

    # overall confidence
    confs = {e.split("[")[-1].rstrip("]") for e in ev}
    if confs <= {"base"}:                       overall = "low (base only)"
    elif "text" in confs and confs <= {"text","base"}: overall = "high"
    elif "knowledge" in confs:                  overall = "medium"
    else:                                        overall = "medium"
    return tags, ev, flags, overall

# ── run ───────────────────────────────────────────────────────────────────────
rows = []
for db, d in EV.items():
    tags, ev, flags, overall = classify(d["name"], d["yiddish"], d["sents"])
    rows.append({
        "db_id": db, "name": d["name"], "name_yiddish": d["yiddish"],
        "n_clusters": d["n_clusters"], "n_sents": len(d["sents"]),
        "cluster_ids": " | ".join(d["cids"]),
        "text_source": d.get("text_source", ""),
        "tags": " | ".join(tags), "confidence": overall,
        "evidence": " ; ".join(ev), "review_flags": " ; ".join(flags),
        "source": "Claude-draft",
    })

# ── validate against Ruthie's 28 ──
def load(p):
    with open(p, newline="", encoding="utf-8") as f: return list(csv.DictReader(f, delimiter="\t"))
ruthie = {r["db_id"]: set(t.strip() for t in (r.get("tags") or "").split("|") if t.strip())
          for r in load("troupe_tags.tsv") if (r.get("tags") or "").strip()}
mine = {r["db_id"]: set(t.strip() for t in r["tags"].split("|") if t.strip()) for r in rows}
tp=fp=fn=0; exact=0; gold=0
for db, rt in ruthie.items():
    if db not in mine: continue
    gold += 1
    mt = mine[db]
    tp += len(rt & mt); fp += len(mt - rt); fn += len(rt - mt)
    if rt == mt: exact += 1
prec = tp/(tp+fp) if tp+fp else 0; rec = tp/(tp+fn) if tp+fn else 0
print(f"=== validation vs Ruthie's {gold} tagged troupes ===")
print(f"exact-set match: {exact}/{gold}")
print(f"tag precision:   {prec:.0%}   tag recall: {rec:.0%}")
print(f"(recall = how many of her tags my rules also picked; precision = how many of mine are hers)")

# write drafts for the UNTAGGED troupes only
untagged = [r for r in rows if r["db_id"] not in ruthie]
cols = ["db_id","name","name_yiddish","n_clusters","n_sents","cluster_ids","text_source","tags","confidence","evidence","review_flags","source"]
with open("troupe_tags_draft.tsv","w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=cols,delimiter="\t"); w.writeheader()
    for r in sorted(untagged,key=lambda x:int(x["db_id"]) if x["db_id"].isdigit() else 1e9):
        w.writerow(r)
print(f"\nwrote troupe_tags_draft.tsv — {len(untagged)} untagged troupes drafted")
from collections import Counter
cc=Counter(r["confidence"] for r in untagged)
print("confidence spread:", dict(cc))
