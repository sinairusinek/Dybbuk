#!/usr/bin/env python3
"""
Phase A2: Cluster proper_name org rows from organizations_classified.tsv.

Strategy:
  1. OpenRefine pre-clusters: rows sharing the same non-empty 'clustered organization'
     value are auto-merged into one cluster.
  2. Within (org_type, settlement) blocks, pick one representative row per cluster,
     then compare representatives pairwise using trigram Jaccard similarity
     (Yiddish-normalised names). Working at cluster level — not row level — handles
     large no-settlement blocks without any size cap.
  3. Pairs ≥ HIGH_THRESHOLD (0.92) → auto-merge.
  4. Pairs in [REVIEW_LOW, HIGH_THRESHOLD) → exported for human review (Zalmen A2 view).

Output:
  organizations_clustered.tsv   — all proper_name rows + cluster_id, canonical_yiddish,
                                  cluster_size, auto_or_review
  cluster_pairs_review.tsv      — uncertain pairs for Zalmen A2 view
"""

import csv, sys, pathlib, unicodedata, re, collections

csv.field_size_limit(sys.maxsize)

# ── Kimatch import ────────────────────────────────────────────────────────────
# Reuse normalize_name (NFD strip diacritics) and name_similarity (trigram Jaccard)
_KIMATCH = pathlib.Path("/Users/sinairusinek/Documents/GitHub/Kimatch")
if _KIMATCH.exists():
    sys.path.insert(0, str(_KIMATCH))
try:
    from kimatch.core.normalizers import normalize_name, name_similarity
except ImportError:
    def normalize_name(name: str) -> str:
        if not name:
            return ""
        nfd = unicodedata.normalize("NFD", name)
        stripped = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
        return re.sub(r"\s+", " ", stripped).strip().lower()

    def name_similarity(a: str, b: str) -> float:
        a, b = normalize_name(a), normalize_name(b)
        if not a or not b:
            return 0.0
        if a == b:
            return 1.0
        def tg(s):
            return {s[i : i + 3] for i in range(len(s) - 2)}
        ta, tb = tg(a), tg(b)
        if not ta or not tb:
            wa, wb = set(a.split()), set(b.split())
            return len(wa & wb) / len(wa | wb) if wa and wb else 0.0
        return len(ta & tb) / len(ta | tb)

# ── Yiddish-specific normalization (applied on top of Kimatch) ────────────────
# Collapses common orthographic variants before similarity comparison.
_YIDDISH_NORM = [
    (re.compile(r"וו"), "ו"),              # double-vov → single
    (re.compile(r"[\u05DA\u05DB]"), "כ"),  # final kaf → kaf
    (re.compile(r"[\u05DF\u05E0]"), "נ"),  # final nun → nun
    (re.compile(r"[\u05E3\u05E4]"), "פ"),  # final pe → pe
    (re.compile(r"[\u05E5\u05E6]"), "צ"),  # final tsadi → tsadi
    (re.compile(r"[\u05DD\u05DE]"), "מ"),  # final mem → mem
]


def normalize_yiddish(name: str) -> str:
    """NFD strip-diacritics (Kimatch) + Yiddish spelling normalizations."""
    s = normalize_name(name)
    for pat, repl in _YIDDISH_NORM:
        s = pat.sub(repl, s)
    return s


# ── Location-aware name helpers ───────────────────────────────────────────────

# Org types that travel; same set as extract_addresses.py EXCLUDED_TYPES.
_TROUPE_TYPES = {
    "troupe", "טרופּע", "טעאַטער-טרופּע", "travelling company",
    "traveling company", "army", "ארמיי", "אַרמיי", "אַרמעע",
    "military", "expedition",
}

def is_stationary_org(org_type: str) -> bool:
    """Return True for fixed-location orgs; False for itinerant types."""
    return org_type.strip().lower() not in _TROUPE_TYPES


_PAREN_RE = re.compile(r"^(.*?)\s*\(([^)]+)\)\s*$")
_AYN_RE   = re.compile(r"^(.*?)\s+אין\s+(.+)$")

def extract_name_location(name: str) -> tuple[str, str]:
    """Split 'Base (City)' or 'Base אין City' → (base, city).
    Returns (name, '') when no qualifier is found."""
    n = name.strip()
    m = _PAREN_RE.match(n)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    m = _AYN_RE.match(n)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return n, ""


def normalize_location(loc: str) -> str:
    """Normalize a city qualifier for comparison.
    Strips Yiddish adjectival -ער suffix, collapses hyphens/spaces."""
    if not loc:
        return ""
    s = re.sub(r"ער$", "", loc.strip())   # ניו-יאָרקער → ניו-יאָרק
    s = re.sub(r"[-\s]+", "", s)           # collapse hyphens and spaces
    return normalize_yiddish(s)


# ── Union-Find ────────────────────────────────────────────────────────────────


class UF:
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        a, b = self.find(a), self.find(b)
        if a != b:
            self.p[b] = a


# ── Config ────────────────────────────────────────────────────────────────────
HIGH_THRESHOLD = 0.92  # auto-merge above this
REVIEW_LOW = 0.80      # flag for human review in [REVIEW_LOW, HIGH_THRESHOLD)

_MISSING = {"", "na", "n/a", "null", "none", "-", "--", "_"}

SRC = pathlib.Path(__file__).with_name("organizations_classified.tsv")
OUT_CLUSTERED = pathlib.Path(__file__).with_name("organizations_clustered.tsv")
OUT_PAIRS = pathlib.Path(__file__).with_name("cluster_pairs_review.tsv")

# ── Column names ──────────────────────────────────────────────────────────────
COL_TITLE = "_ - organizations - _ - title"
COL_CLUSTERED = "clustered organization"
COL_DESC = "_ - organizations - _ - descriptive_name"
COL_ORG_TYPE = "_ - organizations - _ - org_type"
COL_SETTLEMENT = "_ - organizations - _ - locations - _ - settlement"
COL_NAME_TYPE = "name_type"
COL_SENTENCE = "_ - organizations - _ - relations - _ - original_sentence"
COL_XML_ID = "_ - xml:id"
COL_HEADING = "_ - heading"
COL_FILE = "File"
COL_VENUE = "_ - organizations - _ - locations - _ - Venue"
COL_COUNTRY = "_ - organizations - _ - locations - _ - country"
COL_ADDRESS = "_ - organizations - _ - locations - _ - address"


def _is_missing(v: str) -> bool:
    return v.strip().lower() in _MISSING


def best_name(row: dict) -> str:
    """Return the richest available name (clustered > title > desc)."""
    for col in (COL_CLUSTERED, COL_TITLE, COL_DESC):
        v = row.get(col, "").strip()
        if not _is_missing(v):
            return v
    return ""


def location_summary(row: dict) -> str:
    """Compact location string from whatever fields are available in this row."""
    parts = []
    for col in (COL_SETTLEMENT, COL_ADDRESS, COL_VENUE, COL_COUNTRY):
        v = row.get(col, "").strip()
        if not _is_missing(v):
            parts.append(v)
    return " · ".join(dict.fromkeys(parts))  # deduplicate, preserve order


# ── Load ──────────────────────────────────────────────────────────────────────
with open(SRC, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    headers = list(reader.fieldnames)
    all_rows = list(reader)

rows: list[tuple[int, dict]] = [
    (i, r) for i, r in enumerate(all_rows)
    if r.get(COL_NAME_TYPE, "").strip() == "proper_name"
]
print(f"Loaded {len(all_rows)} total rows; {len(rows)} proper_name rows")

N = len(rows)
uf = UF(N)

# ── Step 1: OpenRefine pre-clusters ──────────────────────────────────────────
or_groups: dict[str, list[int]] = collections.defaultdict(list)
for loc_i, (_, r) in enumerate(rows):
    cv = r.get(COL_CLUSTERED, "").strip()
    if not _is_missing(cv):
        or_groups[cv].append(loc_i)

for members in or_groups.values():
    for j in members[1:]:
        uf.union(members[0], j)

print(f"OpenRefine groups: {len(or_groups)} distinct clustered-org values")

# Build per-cluster location summary from ALL rows (not just representative).
# Collected after Step 1 so union-find reflects OpenRefine merges.
# Updated again after Step 2 auto-merges — we rebuild at write time.
def cluster_location_summary(root: int, member_locs: list[int]) -> str:
    """Collect distinct, non-empty location strings across all rows in a cluster."""
    seen: dict[str, None] = {}
    for loc_i in member_locs:
        loc = location_summary(rows[loc_i][1])
        if loc:
            seen[loc] = None
    return " | ".join(seen.keys())  # pipe-separated distinct locations

# ── Step 2: Block-based fuzzy matching (cluster-level) ───────────────────────
# Group rows by (org_type, settlement). Within each block, pick ONE representative
# row per cluster root (already established by Step 1), then compare those
# representatives pairwise. This keeps comparison space proportional to the number
# of distinct organisations in the block, not the number of biographical mentions.


def block_key(r: dict) -> tuple[str, str]:
    org_type = r.get(COL_ORG_TYPE, "").strip().lower()
    settlement = re.sub(r"\s+", " ", r.get(COL_SETTLEMENT, "").strip()).lower()
    # For stationary orgs with no settlement field, fall back to the city
    # qualifier embedded in the name (e.g. "פּיפּלס-טעאַטער (ניו-יאָרקער)").
    # This prevents orgs with different city qualifiers from landing in the
    # same ("theatre", "") block and being wrongly merged.
    if not settlement and is_stationary_org(org_type):
        _, loc = extract_name_location(best_name(r))
        if loc:
            settlement = normalize_location(loc)
    return (org_type, settlement)


blocks: dict[tuple, list[int]] = collections.defaultdict(list)
for loc_i, (_, r) in enumerate(rows):
    blocks[block_key(r)].append(loc_i)

print(f"Blocks: {len(blocks)} (org_type × settlement)")

uncertain_pairs: list[dict] = []
# Deduplicate uncertain pairs by (normalized_name_i, normalized_name_j) order-invariant.
_seen_norm_pairs: dict[tuple[str, str], dict] = {}
auto_merged = 0

for bkey, members in blocks.items():
    if len(members) < 2:
        continue

    # One representative loc per cluster root within this block.
    seen_roots: dict[int, int] = {}
    for loc_i in members:
        root = uf.find(loc_i)
        if root not in seen_roots:
            seen_roots[root] = loc_i
    reps = list(seen_roots.values())

    if len(reps) < 2:
        continue

    for i_idx in range(len(reps)):
        for j_idx in range(i_idx + 1, len(reps)):
            li, lj = reps[i_idx], reps[j_idx]
            ri, rj = rows[li][1], rows[lj][1]

            # Extract base names and city qualifiers separately so that city
            # suffixes don't artificially lower similarity scores.
            bi, li_loc = extract_name_location(best_name(ri))
            bj, lj_loc = extract_name_location(best_name(rj))
            ni = normalize_yiddish(bi)
            nj = normalize_yiddish(bj)
            if not ni or not nj:
                continue

            li_norm = normalize_location(li_loc)
            lj_norm = normalize_location(lj_loc)
            # True when both names carry explicit but different city qualifiers.
            loc_conflict = bool(li_norm and lj_norm and li_norm != lj_norm)

            sim = 1.0 if ni == nj else name_similarity(ni, nj)

            def _pair_record(lc=loc_conflict):
                return {
                    "loc_i": li,
                    "loc_j": lj,
                    "name_i": best_name(ri),
                    "name_j": best_name(rj),
                    "org_type": bkey[0],
                    "settlement": bkey[1],
                    "similarity": round(sim, 4),
                    "location_conflict": lc,
                    "entry_id_i": ri.get(COL_XML_ID, ""),
                    "entry_id_j": rj.get(COL_XML_ID, ""),
                    "file_i": ri.get(COL_FILE, ""),
                    "file_j": rj.get(COL_FILE, ""),
                    "heading_i": ri.get(COL_HEADING, ""),
                    "heading_j": rj.get(COL_HEADING, ""),
                    "sentence_i": ri.get(COL_SENTENCE, ""),
                    "sentence_j": rj.get(COL_SENTENCE, ""),
                    "location_i": location_summary(ri),
                    "location_j": location_summary(rj),
                }

            if sim >= HIGH_THRESHOLD:
                if loc_conflict:
                    # Same base name but different city qualifiers → flag for review,
                    # do NOT auto-merge.
                    norm_key = tuple(sorted((ni, nj)))
                    existing = _seen_norm_pairs.get(norm_key)
                    if existing is None or sim > existing["similarity"]:
                        _seen_norm_pairs[norm_key] = _pair_record()
                else:
                    uf.union(li, lj)
                    auto_merged += 1
            elif sim >= REVIEW_LOW:
                norm_key = tuple(sorted((ni, nj)))
                existing = _seen_norm_pairs.get(norm_key)
                if existing is None or sim > existing["similarity"]:
                    _seen_norm_pairs[norm_key] = _pair_record()

uncertain_pairs = list(_seen_norm_pairs.values())

print(f"Auto-merged pairs: {auto_merged}")
print(f"Uncertain pairs for review: {len(uncertain_pairs)}")

# ── Step 3: Assign cluster IDs + canonical names ──────────────────────────────

cluster_members: dict[int, list[int]] = collections.defaultdict(list)
for loc_i in range(N):
    cluster_members[uf.find(loc_i)].append(loc_i)

root_to_cid: dict[int, str] = {
    root: f"ORG-C{cid_num + 1:05d}"
    for cid_num, root in enumerate(sorted(cluster_members))
}


def canonical_for(member_locs: list[int]) -> str:
    """Pick canonical Yiddish name: most-common clustered-org value, else most-common title."""
    clustered_vals = [
        rows[l][1].get(COL_CLUSTERED, "").strip()
        for l in member_locs
    ]
    non_empty = [v for v in clustered_vals if not _is_missing(v)]
    if non_empty:
        return collections.Counter(non_empty).most_common(1)[0][0]
    names = [best_name(rows[l][1]) for l in member_locs if best_name(rows[l][1])]
    if names:
        return collections.Counter(names).most_common(1)[0][0]
    return ""


cluster_canonical: dict[int, str] = {
    root: canonical_for(members)
    for root, members in cluster_members.items()
}

# ── Step 4: Write organizations_clustered.tsv ─────────────────────────────────
extra_cols = ["cluster_id", "canonical_yiddish", "cluster_size", "auto_or_review"]
out_headers = headers + extra_cols

with open(OUT_CLUSTERED, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(
        f, fieldnames=out_headers, delimiter="\t", extrasaction="ignore"
    )
    writer.writeheader()
    for loc_i, (_, r) in enumerate(rows):
        root = uf.find(loc_i)
        size = len(cluster_members[root])
        row_out = dict(r)
        row_out["cluster_id"] = root_to_cid[root]
        row_out["canonical_yiddish"] = cluster_canonical[root]
        row_out["cluster_size"] = size
        row_out["auto_or_review"] = "singleton" if size == 1 else "auto"
        writer.writerow(row_out)

n_singletons = sum(1 for m in cluster_members.values() if len(m) == 1)
print(f"\nWrote {N} rows → {OUT_CLUSTERED.name}")
print(
    f"Clusters: {len(cluster_members)} total "
    f"({n_singletons} singletons, {len(cluster_members) - n_singletons} multi-row)"
)

# ── Step 5: Write cluster_pairs_review.tsv ────────────────────────────────────
pair_headers = [
    "pair_id",
    "cluster_id_i",
    "cluster_id_j",
    "name_i",
    "name_j",
    "org_type",
    "settlement",
    "similarity",
    "location_conflict",   # TRUE when names carry different explicit city qualifiers
    "entry_id_i",
    "entry_id_j",
    "file_i",
    "file_j",
    "heading_i",
    "heading_j",
    "sentence_i",
    "sentence_j",
    "location_i",          # extracted location data for name_i's representative row
    "location_j",          # extracted location data for name_j's representative row
    "decision",            # MERGE / SPLIT / DEFER — filled by Zalmen app
    "reviewer_settlement", # reviewer-supplied settlement for merged cluster
    "reviewer_address",    # reviewer-supplied address for merged cluster
    "reviewer_notes",
]

with open(OUT_PAIRS, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=pair_headers, delimiter="\t")
    writer.writeheader()
    for k, pair in enumerate(
        sorted(uncertain_pairs, key=lambda p: -p["similarity"])
    ):
        li, lj = pair["loc_i"], pair["loc_j"]
        writer.writerow(
            {
                "pair_id": f"PAIR-{k + 1:05d}",
                "cluster_id_i": root_to_cid[uf.find(li)],
                "cluster_id_j": root_to_cid[uf.find(lj)],
                "name_i": pair["name_i"],
                "name_j": pair["name_j"],
                "org_type": pair["org_type"],
                "settlement": pair["settlement"],
                "similarity": pair["similarity"],
                "location_conflict": "TRUE" if pair.get("location_conflict") else "",
                "entry_id_i": pair["entry_id_i"],
                "entry_id_j": pair["entry_id_j"],
                "file_i": pair["file_i"],
                "file_j": pair["file_j"],
                "heading_i": pair["heading_i"],
                "heading_j": pair["heading_j"],
                "sentence_i": pair["sentence_i"],
                "sentence_j": pair["sentence_j"],
                "location_i": pair.get("location_i", ""),
                "location_j": pair.get("location_j", ""),
                "decision": "",
                "reviewer_settlement": "",
                "reviewer_address": "",
                "reviewer_notes": "",
            }
        )

print(f"Wrote {len(uncertain_pairs)} pairs → {OUT_PAIRS.name}")
