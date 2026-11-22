"""Kind of a GENERIC org cluster: what sort of institution a non-individuating name names.

Sinai 2026-11-22 (generic type-clustering, option "kind as a property"): a GENERIC
cluster never becomes a node of any kind. Its mentions keep two things the corpus can
answer questions with: the KIND ("a yeshiva", "a gymnasium", "a Yiddish theatre") and,
when the mention names one, the PLACE. The KG records them as a person -> place edge
(`generic_org_in`, kind in role_detail) and in kg/generic_mentions.tsv. No person ->
person path can run through a kind, because a kind is never a node.

Vocabulary, closed and two-level:
  - Education rows get a fine kind (yeshiva, kheyder, gymnasium, ...): the institutions
    the Leksikon names most often and most vaguely, and where the kind is the finding.
  - Every other row's kind is its org_type slug (theatre, military, press, ...): the
    org_type vocabulary already is the kind vocabulary for those.
  Corrections: generic_kind_overrides.tsv (cluster_id, kind, note) wins over the rules.

The troupe tags (troupe_tags.tsv) are the mirror image: they describe MINTED troupes
(db_id) along a 16-tag vocabulary. A generic troupe cluster ("a Yiddish troupe") gets
kind `troupe` here and no troupe tags: there is no entity to tag.

Names are compared after sm(): points stripped, final letters folded.
"""
from __future__ import annotations
import csv, re, unicodedata
from pathlib import Path

HERE = Path(__file__).resolve().parent
OVERRIDES_TSV = HERE / "generic_kind_overrides.tsv"


def sm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s.translate(str.maketrans("ךםןףץ", "כמנפצ"))


# Education fine kinds, first match wins (patterns are written in sm() form:
# no points, medial letters only).
_EDU = [
    ("yeshiva", r"ישיב|ישיוו|(^|[^י])שיבה"),
    ("kheyder", r"חדר|חיידר|חדיר"),
    ("talmud-torah", r"תלמוד.?ת[ו]?[י]?ר"),
    ("beys-medresh", r"בית.?המדרש|קלויז"),
    ("gymnasium", r"גימנ[א]?[ז]?י|גימאזי|גמנסי|ליציי|ליצעי|ליצעא"),
    ("realschule", r"רעאל"),
    ("conservatory", r"קאנסערוואטאר|קאנסערוואטור"),
    ("drama-school", r"דראמ|טעאטער.?שול|טעאטראל|טעאטער.?סטודיע|בינע.?שול"),
    ("music-school", r"מוזיק|מוזיקאל"),
    ("art-school", r"קונסט|צייכנ|מאלער|באלעט|טאנצ"),
    ("rabbinical-seminary", r"ראבינער"),
    ("teachers-seminary", r"סעמינאר|לערער"),
    ("university", r"אוניווערסיטעט|אוניווערזיטעט|יוניווערסיטי"),
    ("academy", r"אקאדעמי"),
    ("polytechnic", r"טעכניק|טעכניש|פאליטעכ|טעכנאלאג"),
    ("courses", r"קורס"),
    ("institute", r"אינסטיטוט"),
    ("commercial-school", r"האנדל|קאמערצ|קאמערציע|פאכ|מעסער|האנטווערק|פראפעסיאנעל|בוכהאלט"),
    ("studio", r"סטודיע"),
    ("college", r"קאלעדזש|קאלעגי"),
    ("camp", r"קעמפ|קאלאניע"),
    ("high-school", r"ה[יא]י.?סקול|האיקול|מיטל.?שול|ה[וי]יכ.?שול|האכשול"),
    ("government-school", r"רעגירונג|קרויניש|קאזיאנ"),
    ("evening-school", r"אוונט|נייט.?סקול"),
    ("elementary-school", r"פאלקס.?שול|שטאטיש|עלעמענטאר|פרימאר|פאבליק|ציבור|קלאסיק|אנפאנג|קינדער|כלל|גרונט|אוטשילישטש|נארמאל"),
    ("school", r"שול|סקול|שקאלע|בית.?ספר"),
]
_EDU = [(k, re.compile(p)) for k, p in _EDU]


def org_type_slug(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return "unknown"
    t = re.sub(r"\(.*?\)", "", t)
    t = t.replace("OTHER - elaborate!", "other").replace("Journals/ Newspapers", "press")
    return re.sub(r"[^a-z0-9]+", "-", t.lower()).strip("-") or "unknown"


def classify(canonical: str, org_type: str) -> str:
    if (org_type or "").strip() == "Education":
        s = sm(canonical)
        for kind, rx in _EDU:
            if rx.search(s):
                return kind
        return "education-other"
    return org_type_slug(org_type)


def load_overrides() -> dict[str, str]:
    if not OVERRIDES_TSV.exists():
        return {}
    with OVERRIDES_TSV.open(encoding="utf-8", newline="") as f:
        return {r["cluster_id"].strip(): r["kind"].strip()
                for r in csv.DictReader(f, delimiter="\t") if r.get("kind", "").strip()}


def kinds_for(review_rows) -> dict[str, str]:
    """cluster_id -> kind for every GENERIC row of org_alignment_review."""
    ov = load_overrides()
    out = {}
    for r in review_rows:
        if (r.get("decision") or "").strip() != "GENERIC":
            continue
        cid = (r.get("cluster_id") or "").strip()
        out[cid] = ov.get(cid) or classify(r.get("canonical_yiddish", ""), r.get("org_type", ""))
    return out


if __name__ == "__main__":
    import collections, sys
    csv.field_size_limit(sys.maxsize)
    with (HERE / "org_alignment_review.tsv").open(encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    k = kinds_for(rows)
    canon = {r["cluster_id"].strip(): r["canonical_yiddish"] for r in rows}
    c = collections.Counter(k.values())
    for kind, n in c.most_common():
        ex = [canon[cid] for cid, v in k.items() if v == kind][:6]
        print(f"{n:4d}  {kind:22s} {' · '.join(ex)}")
