#!/usr/bin/env python3
"""Apply the confirmed false-positive fixes found by the translit-mismatch /
WD-cross-check / ambiguity audits (2026-05-22 Dybbuk session).

Two error classes, fixed in their CANONICAL upstream sources so the fixes survive
a rebuild of toponyms_attestations.csv:

  Class A  person-corpus mention mis-resolution (places_unified_corrected.csv):
           a minority of attestations of a country/city name were resolved to a
           random Kima place, overriding the correct qid_map entry. We rewrite
           the qid for the exact (source_value, wrong_qid) rows.
  Class B  kima_name_exact devocalized-collision FP (unlinked_confirmed.tsv):
           the auto-linker matched a Yiddish spelling to a colliding Hebrew Kima
           variant of an unrelated place. We rewrite the qid + clear the wrong
           Kima fields and tag method=audit_corrected.

Every change is appended to matching_corrections_log.tsv. Run with system python.
"""
import csv, sys
from pathlib import Path

csv.field_size_limit(10**7)
WORK = Path(__file__).resolve().parent.parent / "data" / "working"
KIMA = WORK / "kima"
UNIFIED = WORK / "places_unified_corrected.csv"
CONFIRMED = KIMA / "unlinked_confirmed.tsv"
LOG = KIMA / "matching_corrections_log.tsv"

# qid -> canonical metadata (verified live against Wikidata 2026-05-22)
META = {
    "Q159": ("Russia", "רוסלאנד", "Q6256", "countries"),
    "Q218": ("Romania", "רומעניע", "Q6256", "countries"),
    "Q28":  ("Hungary", "אונגארן", "Q6256", "countries"),
    "Q36":  ("Poland", "פוילן", "Q6256", "countries"),
    "Q16":  ("Canada", "קאנאדע", "Q6256", "countries"),
    "Q19313": ("Barysaw", "באריסאב", "Q1549591", "settlements"),
    "Q140147": ("Brest", "בריסק", "Q1549591", "settlements"),
    "Q156588": ("Tysmenytsia", "", "Q1549591", "settlements"),
    "Q19566": ("Simferopol", "", "Q1549591", "settlements"),
    "Q189074": ("Harlem", "", "Q1549591", "settlements"),
    "Q976": ("Tomsk", "", "Q1549591", "settlements"),
    "Q48256": ("Dnipro", "דניפרא", "Q1549591", "settlements"),
}

# Class A: (source_value, wrong_qid) -> correct_qid   [places_unified_corrected.csv]
CLASS_A = {
    ("רוסלאַנד", "Q900"): "Q159",       # Russia (was Kazan)
    ("רומעניע", "Q596609"): "Q218",     # Romania (was Vișeu de Sus)
    ("אונגאַרן", "Q181276"): "Q28",      # Hungary (was Szolnok)
    ("פּוילן", "Q104725"): "Q36",        # Poland (was Płock)
    ("קאָנאָדע", "Q92561"): "Q16",        # Canada (was London, Ont.)
    ("באָריסאָוו", "Q207294"): "Q19313",  # Barysaw/Borisov (was Babruysk)
    ("בריסק ליטאָווסק", "Q12193"): "Q140147",  # Brest, Belarus (was Brest, France)
    ("טישמיעניץ", "Q973977"): "Q156588",  # Tysmenytsia (was Abirim, Israel)
    ("סימפעראָפּאָל", "Q43421"): "Q19566",  # Simferopol (was Richmond, Va.)
    ("האַרלעם", "Q41183"): "Q189074",     # Harlem, NYC (was Aleppo, Syria)
}

# Class B: spelling -> correct_qid   [unlinked_confirmed.tsv; clears wrong Kima fields]
CLASS_B = {
    "טאָמסק": "Q976",                    # Tomsk (was Q5687 / Jericho kima)
    "דניעפּראָפּיעטראָווסק": "Q48256",       # Dnipro (was Q43295 / Villeneuve-d'Ascq)
    "האַרלעם": "Q189074",                # Harlem (was Q41183 / Aleppo)
}

log_rows = []  # (yiddish, wrong_qid, wrong_label, correct_qid, correct_label, cause)

# ── Class A ────────────────────────────────────────────────────────────────────
rows = list(csv.DictReader(open(UNIFIED)))
fields = rows[0].keys()
nA = 0
for r in rows:
    key = (r["source_value"].strip(), r["qid"].strip())
    cq = CLASS_A.get(key)
    if not cq:
        continue
    en, yi, typ, cat = META[cq]
    log_rows.append((key[0], key[1], r.get("wikidata_label_en", ""), cq, en,
                     "translit_audit/person_mention_misresolution"))
    r["qid"] = cq
    r["qid_source"] = "translit_audit_2026-05-22"
    r["wikidata_label_en"] = en
    r["wikidata_label_yi"] = yi
    r["wikidata_type"] = typ
    r["resolved_category"] = cat
    if "correction_applied" in r:
        r["correction_applied"] = "True"
    nA += 1
with open(UNIFIED, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(fields)); w.writeheader(); w.writerows(rows)
print(f"Class A: rewrote {nA} rows in {UNIFIED.name}", file=sys.stderr)

# ── Class B ────────────────────────────────────────────────────────────────────
crows = list(csv.DictReader(open(CONFIRMED), delimiter="\t"))
cfields = crows[0].keys()
nB = 0
for r in crows:
    cq = CLASS_B.get(r["yiddish"].strip())
    if not cq or r["qid"].strip() == cq:
        continue
    en, yi, typ, cat = META[cq]
    log_rows.append((r["yiddish"].strip(), r["qid"].strip(), r.get("kima_rom", ""), cq, en,
                     "kima_name_exact/devocalized_collision"))
    r["qid"] = cq
    for fld in ("kima_id", "kima_rom", "kima_heb", "lat", "lon"):
        if fld in r:
            r[fld] = ""               # wrong Kima record; let rebuild leave blank
    r["label_en"] = en
    r["method"] = "audit_corrected"
    nB += 1
with open(CONFIRMED, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(cfields), delimiter="\t")
    w.writeheader(); w.writerows(crows)
print(f"Class B: rewrote {nB} rows in {CONFIRMED.name}", file=sys.stderr)

# ── append to corrections registry ─────────────────────────────────────────────
with open(LOG, "a", newline="") as f:
    w = csv.writer(f, delimiter="\t")
    for row in log_rows:
        w.writerow(row)
print(f"logged {len(log_rows)} corrections to {LOG.name}", file=sys.stderr)
