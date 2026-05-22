#!/usr/bin/env python3
"""Audit ALL linked toponym attestations (not just kima_name_exact) for false
positives, generalizing the kima_name_exact audit to every link_method.

Run with the Kimatch venv (has abydos + the dybbuk Yiddish->IPA bridge + kimatch):
    ~/Documents/GitHub/Kimatch/.venv/bin/python scripts/audit_all_links.py

Two offline passes (no network):
  A. TRANSLIT-MISMATCH  — the wrong-city catcher. The oracle is the linked place's
     OWN Kima names: for each linked Hebrew/Yiddish spelling we check whether it
     matches ANY attested name of the linked place (its Hebrew/Yiddish variants
     OR its Latin romanization), by normalized-exact OR Daitch-Mokotoff overlap
     via the vocalization-aware Yiddish proxy. Matching the modern English name is
     NOT required — Yiddish exonyms (פּוילן→Poland, לעמבערג→Lviv) pass because Kima
     records them as variants. A spelling that matches none of the linked place's
     own names is flagged.  (handoff item 2)
  B. KIMA-AMBIGUITY     — for each linked spelling, look it up in the Kima
     variant index; if the normalized spelling maps to >1 distinct place, flag
     it with the alternatives so a human can confirm the pick.  (handoff item 1)

Scope is every attestation with link_status==linked and a qid, across all
link_methods. Outputs go to data/working/kima/.
"""
import csv, re, sys, unicodedata
from pathlib import Path
from collections import defaultdict, Counter
csv.field_size_limit(10**7)

ZIBN = Path(__file__).resolve().parent.parent
WORK = ZIBN / "data" / "working"
KIMA = WORK / "kima"
KREPO = Path.home() / "Documents" / "GitHub" / "Kimatch"
PLACES_CSV = KREPO / "20250126KimaPlacesCSVx.csv"
VARIANTS_TSV = KREPO / "Kima-Variants-20250929.tsv"

from kimatch.core.phonetics import strategy_for
from abydos.phonetic import DaitchMokotoff
_dm = DaitchMokotoff()
_yi = strategy_for("yiddish")

# ── normalizers ──────────────────────────────────────────────────────────────
HEB_ANY = re.compile(r"[א-ת]")

def heb_norm(s: str) -> str:
    """Strip niqqud/cantillation, geresh/gershayim, spaces, hyphens, punctuation."""
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[֑-ׇ׳״'\"`.,()\[\]‘’“”/]+", "", s)
    return re.sub(r"[\s\-־–—]+", "", s).strip()

def lat_main(s: str) -> str:
    """Latin romanization -> main token: drop parenthetical / after comma, deaccent."""
    s = re.split(r"[(,:]", s or "", 1)[0]
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^A-Za-z ]+", "", s).strip()

def dm_codes(latin: str) -> set:
    latin = (latin or "").replace(" ", "")
    if not latin:
        return set()
    try:
        return set(_dm.encode(latin))
    except Exception:
        return set()

def bigrams(s):
    s = re.sub(r"[^a-z]", "", (s or "").lower())
    return {s[i:i+2] for i in range(len(s)-1)} or ({s} if s else set())

def dice(a, b):
    A, B = bigrams(a), bigrams(b)
    return 2*len(A & B)/(len(A)+len(B)) if (A or B) else 0.0

# ── load attestations ─────────────────────────────────────────────────────────
att = list(csv.DictReader(open(WORK/"toponyms_attestations.csv")))
linked = [a for a in att if a["link_status"] == "linked" and a["qid"]]

# distinct (spelling, qid) with occurrence counts + method + kima_rom
groups = {}
for a in linked:
    k = (a["source_value"], a["qid"])
    g = groups.setdefault(k, {"occ": 0, "kima_rom": a["kima_rom"], "label_en": a["label_en"],
                              "methods": Counter(), "script": a["source_value_script"]})
    g["occ"] += 1
    g["methods"][a["link_method"] or "(blank)"] += 1
print(f"linked attestations: {len(linked)}  distinct (spelling,qid): {len(groups)}", file=sys.stderr)

# ── build Kima index (places + variants) — used by both passes ─────────────────
pid_qid, pid_rom, pid_heb = {}, {}, {}
qid_pids = defaultdict(set)
for r in csv.DictReader(open(PLACES_CSV)):
    q = (r.get("WikiData_Id") or "").strip()
    pid_qid[r["id"]] = q
    pid_rom[r["id"]] = (r.get("primary_rom_full") or "").strip()
    pid_heb[r["id"]] = (r.get("primary_heb_full") or "").strip()
    if q:
        qid_pids[q].add(r["id"])
# normalized Hebrew name -> set of PlaceIds  (for ambiguity lookups)
var_index = defaultdict(set)
# PlaceId -> set of its Hebrew/Yiddish variant strings  (for the wrong-city oracle)
pid_variants = defaultdict(set)
for r in csv.DictReader(open(VARIANTS_TSV), delimiter="\t"):
    pid = r["PlaceId"]
    for field in ("primary_heb_full", "variant"):
        s = r.get(field) or ""
        if HEB_ANY.search(s):
            pid_variants[pid].add(s)
        n = heb_norm(s)
        if n:
            var_index[n].add(pid)
# also fold each place's own primary_heb_full (from places CSV) into its variants
for pid, h in pid_heb.items():
    if HEB_ANY.search(h):
        pid_variants[pid].add(h)
    n = heb_norm(h)
    if n:
        var_index[n].add(pid)
print(f"kima index: {len(var_index)} normalized name keys, "
      f"{sum(len(v) for v in pid_variants.values())} place-variant strings", file=sys.stderr)

def place_signature(qid):
    """All (normalized-name set, DM-code set) attested for a QID's Kima place(s)."""
    norms, dm = set(), set()
    for pid in qid_pids.get(qid, ()):
        for v in pid_variants.get(pid, ()):       # Hebrew/Yiddish variants
            norms.add(heb_norm(v))
            for p in (_yi.all_proxies(v) or []):
                dm |= dm_codes(p)
        rom = pid_rom.get(pid, "")                # Latin romanization
        if rom:
            dm |= dm_codes(lat_main(rom))
    norms.discard("")
    return norms, dm

# ── PASS A: translit-mismatch (oracle = the linked place's own Kima names) ─────
HEB = re.compile(r"[א-ת]")
_sig_cache = {}
mism = []
for (sv, qid), g in groups.items():
    if not HEB.search(sv):
        continue                       # only Hebrew-script spellings
    if qid not in qid_pids:
        continue                       # place not in this Kima snapshot -> can't judge
    sig_norms, sig_dm = _sig_cache.get(qid) or _sig_cache.setdefault(qid, place_signature(qid))
    nsv = heb_norm(sv)
    if nsv in sig_norms:
        continue                       # exact variant of the linked place -> fine
    proxies = _yi.all_proxies(sv) or []
    dm_sv = set()
    for p in proxies:
        dm_sv |= dm_codes(p)
    if dm_sv & sig_dm:
        continue                       # sounds like one of the place's names -> fine
    # also tolerate close string overlap to the romanization (handles short names)
    rom_main = lat_main(g["kima_rom"] or g["label_en"])
    sim = max((dice(p, rom_main) for p in proxies), default=0.0)
    if sim >= 0.5:
        continue
    sev = "STRONG" if sim < 0.3 else "WEAK"
    mism.append({"severity": sev, "occ": g["occ"], "yiddish": sv,
                 "yi_proxy": proxies[0] if proxies else "", "qid": qid,
                 "kima_rom": g["kima_rom"] or g["label_en"], "dice_rom": round(sim, 2),
                 "n_kima_variants": len(sig_norms),
                 "methods": "|".join(f"{m}:{n}" for m, n in g["methods"].most_common())})
mism.sort(key=lambda r: (r["severity"] != "STRONG", -r["occ"]))
with open(KIMA/"audit_translit_mismatch.tsv", "w", newline="") as f:
    cols = ["severity","occ","yiddish","yi_proxy","qid","kima_rom","dice_rom",
            "n_kima_variants","methods"]
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(mism)
print(f"[A] translit-mismatch flags: {len(mism)} "
      f"(STRONG={sum(r['severity']=='STRONG' for r in mism)})  occ="
      f"{sum(r['occ'] for r in mism)}", file=sys.stderr)

amb = []
for (sv, qid), g in groups.items():
    n = heb_norm(sv)
    if not n:
        continue
    pids = var_index.get(n, set())
    qids = {pid_qid.get(p, "") for p in pids}
    qids.discard("")
    if len(qids) <= 1:
        continue
    # build readable options: QID -> a sample rom
    opts = []
    for p in sorted(pids, key=lambda x: pid_rom.get(x, "")):
        q = pid_qid.get(p, "-")
        opts.append(f"{pid_rom.get(p,'?')}[{p}->{q}]")
    amb.append({"occ": g["occ"], "yiddish": sv, "linked_qid": qid,
                "linked_label": g["label_en"], "n_places": len(qids),
                "linked_in_set": "yes" if qid in qids else "NO",
                "options": " | ".join(opts[:6]),
                "methods": "|".join(f"{m}:{n2}" for m, n2 in g["methods"].most_common())})
amb.sort(key=lambda r: (r["linked_in_set"] == "yes", -r["occ"]))
with open(KIMA/"audit_ambiguity_all.tsv", "w", newline="") as f:
    cols = ["occ","yiddish","linked_qid","linked_label","n_places","linked_in_set","options","methods"]
    w = csv.DictWriter(f, fieldnames=cols, delimiter="\t"); w.writeheader(); w.writerows(amb)
print(f"[B] kima-ambiguity flags: {len(amb)} "
      f"(linked-NOT-in-set={sum(r['linked_in_set']=='NO' for r in amb)})", file=sys.stderr)
