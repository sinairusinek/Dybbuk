"""Audit every date in the plays KG for impossibility.

READ-ONLY. Writes one report and changes nothing.

The Lexicon is a closed historical source: Zylbercweig's volumes went to press
between the 1930s and 1971, so nothing in it can postdate its own volume, and
no one in it lived 130 years. Any date that breaks those bounds is an error in
OUR data, not a fact about the world.

Three places hold dates, and all three are checked:

  * `kg/nodes.tsv`  — `attrs.birth_date` / `attrs.death_date` (the vitals)
  * `kg/edges.tsv`  — `date_start` / `date_end` on any relation
  * `kg/events.tsv` — `date_start` / `date_end` per event

Four checks:

  LATE          a year after the volume's own publication ceiling
  REVERSED      death before birth
  LIFESPAN      death - birth > MAX_AGE
  EARLY         a year before MIN_YEAR (a 3- or 4-digit OCR mangling)
  OUTSIDE_LIFE  an edge dated before a person's birth, or after their death
                for a relation that cannot outlive them (posthumous
                publication and production are expected and NOT flagged)

IMPRECISE notation is NOT an error and is reported separately: `189X`, `188..`,
`19XX`, `1852~`, `1914/1915` and bare `189` are the editors' own way of saying
"we don't know the exact year". A checker that reads `189` as the year 189 will
invent ~60 phantom errors; `decade_floor()` exists so that never happens.

The ceiling is per volume, because a volume cannot mention a death that had not
happened when it was printed. Volume 7 (1971) legitimately carries a 1971
death; volume 1 (1931) carrying a 1992 one is an OCR error.

    python3.11 audit_dates.py            # summary to stdout
    python3.11 audit_dates.py --tsv      # also write qa/date_issues.tsv
"""
import csv, json, re, sys, collections, os

csv.field_size_limit(10**9)
HERE = os.path.dirname(os.path.abspath(__file__))
KG = os.path.join(HERE, "kg")

# Latest year each volume can plausibly mention. These are deliberately LOOSE:
# volumes 1-2 were supplemented and reissued for decades after first printing,
# so a death well after the first edition is normal (Gilrod, vol 1, d. 1939).
# A ceiling tight to the first printing produces false positives, so each is
# set past the volume's own corrections, and only an absurd year is an error.
VOLUME_CEILING = {"1": 1950, "2": 1950, "3": 1959, "4": 1963,
                  "5": 1967, "6": 1969, "7": 1971}
DEFAULT_CEILING = 1971          # unknown volume: the latest the work can go
MAX_AGE = 105
MIN_YEAR = 1700

# Relations that legitimately outlive the person: the work keeps circulating.
POSTHUMOUS_OK = {"work_published_by", "work_produced_by", "authored",
                 "of_play", "involved_in", "staged_at", "cast_in",
                 "premiere_of", "translated"}

# Notation the editors use for a date they do not know exactly. These are
# imprecise, not wrong, and must never be read as a literal year.
IMPRECISE = re.compile(r"""^(
      \d{3}[Xx]            # 189X  -> the 1890s
    | \d{3}                # 189   -> the 1890s, truncated
    | \d{2}[Xx.]{2}        # 19XX / 19..
    | \d{3}[.~?]+          # 188.. / 189~
    | \d{2}                # 18    -> the 19th century
    | [Xx?.]+\d*           # XXXX-07-18 / ..1914
)""", re.VERBOSE)


def decade_floor(raw):
    """Earliest year a date string can mean, or None if it names no year.

    `189X` and `189` both mean the 1890s, so they floor to 1890 -- never 189.
    """
    s = str(raw or "").strip()
    if not s:
        return None, False
    m = re.match(r"^(\d{4})", s)
    if m:                                   # a real 4-digit year
        return int(m.group(1)), bool(re.search(r"[~?/]|\.\.", s))
    m = re.match(r"^(\d{3})[Xx.~?]*$", s)   # 189X / 189 / 189~ -> 1890
    return (int(m.group(1)) * 10, True) if m else (None, True)


def ceiling(volume):
    return VOLUME_CEILING.get(str(volume or "").strip(), DEFAULT_CEILING)


def rows(name):
    path = os.path.join(KG, name)
    with open(path, encoding="utf-8") as fh:
        yield from csv.DictReader(fh, delimiter="\t")


def main():
    issues, imprecise = [], collections.Counter()
    births, deaths, labels, vols = {}, {}, {}, {}

    def add(where, ident, label, field, value, problem, kind):
        issues.append((kind, where, ident, label, field, value, problem))

    # ---- 1. the vitals on person nodes -------------------------------------
    for r in rows("nodes.tsv"):
        attrs = (r.get("attrs") or "").strip()
        if not attrs.startswith("{"):
            continue
        try:
            d = json.loads(attrs)
        except ValueError:
            continue
        nid = r["node_id"]
        labels[nid] = r.get("label_english") or r.get("label_yiddish") or ""
        vols[nid] = d.get("volume")
        cap = ceiling(d.get("volume"))

        parsed = {}
        for field in ("birth_date", "death_date"):
            raw = d.get(field)
            if raw is None or not str(raw).strip():
                continue
            if IMPRECISE.match(str(raw).strip()):
                imprecise[field] += 1
            year, vague = decade_floor(raw)
            if year is None:
                imprecise[field + " (unparseable)"] += 1
                continue
            parsed[field] = year
            if year > cap:
                add("node_attrs", nid, labels[nid], field, raw,
                    f"year {year} after vol {d.get('volume')} ceiling {cap}", "LATE")
            elif year < MIN_YEAR:
                add("node_attrs", nid, labels[nid], field, raw,
                    f"year {year} before {MIN_YEAR}", "EARLY")

        b, dt = parsed.get("birth_date"), parsed.get("death_date")
        if b:
            births[nid] = (b, d.get("birth_date"))
        if dt:
            deaths[nid] = (dt, d.get("death_date"))
        if b and dt:
            if dt < b:
                add("node_attrs", nid, labels[nid], "lifespan",
                    f"{d.get('birth_date')} -> {d.get('death_date')}",
                    f"death {dt - b} years before birth", "REVERSED")
            elif dt - b > MAX_AGE:
                add("node_attrs", nid, labels[nid], "lifespan",
                    f"{d.get('birth_date')} -> {d.get('death_date')}",
                    f"age {dt - b} > {MAX_AGE}", "LIFESPAN")

    # ---- 2. dated edges ----------------------------------------------------
    for r in rows("edges.tsv"):
        desc = f'{r["source_id"]} -{r["edge_type"]}-> {r["target_id"]}'
        for col in ("date_start", "date_end"):
            year, _ = decade_floor(r.get(col))
            if year is None:
                continue
            if year > DEFAULT_CEILING:
                add("edge", r["edge_id"], desc, col, r.get(col),
                    f"year {year} after {DEFAULT_CEILING}", "LATE")
            elif year < MIN_YEAR:
                add("edge", r["edge_id"], desc, col, r.get(col),
                    f"year {year} before {MIN_YEAR}", "EARLY")

        start, _ = decade_floor(r.get("date_start"))
        if start is None:
            continue
        for pid in (r["source_id"], r["target_id"]):
            if not pid.startswith("person:"):
                continue
            # A birth/death event edge legitimately carries that very date.
            if r.get("event_id", "").startswith(("event:birth", "event:death")):
                continue
            # Posthumous publication and production are ordinary facts: a play
            # is reprinted and restaged for decades after its author dies.
            # Only a date before BIRTH is suspect for these.
            if r["edge_type"] in POSTHUMOUS_OK and pid in deaths:
                if start >= deaths[pid][0]:
                    continue
            if pid in births and start < births[pid][0] - 1:
                add("edge_vs_life", r["edge_id"],
                    f"{labels.get(pid, pid)} ({pid})", r["edge_type"],
                    r.get("date_start"),
                    f"before birth {births[pid][1]}", "OUTSIDE_LIFE")
            if pid in deaths and start > deaths[pid][0] + 1:
                add("edge_vs_life", r["edge_id"],
                    f"{labels.get(pid, pid)} ({pid})", r["edge_type"],
                    r.get("date_start"),
                    f"after death {deaths[pid][1]}", "OUTSIDE_LIFE")

    # ---- 3. events ---------------------------------------------------------
    for r in rows("events.tsv"):
        for col in ("date_start", "date_end"):
            year, _ = decade_floor(r.get(col))
            if year is None:
                continue
            if year > DEFAULT_CEILING or year < MIN_YEAR:
                add("event", r["event_id"], r.get("event_type", ""), col,
                    r.get(col), f"year {year} outside [{MIN_YEAR},{DEFAULT_CEILING}]",
                    "LATE" if year > DEFAULT_CEILING else "EARLY")

    # ---- report -----------------------------------------------------------
    print(f"vitals: {len(births)} birth dates, {len(deaths)} death dates")
    print(f"imprecise-but-valid notation (not errors): {sum(imprecise.values())}")
    for k, c in imprecise.most_common():
        print(f"    {c:5d}  {k}")
    print(f"\nISSUES: {len(issues)}")
    for kind, c in collections.Counter(i[0] for i in issues).most_common():
        print(f"    {c:5d}  {kind}")

    hard = [i for i in issues if i[0] != "OUTSIDE_LIFE"]
    if hard:
        print(f"\n--- {len(hard)} impossible dates (fix at the source) ---")
        for kind, where, ident, label, field, value, problem in sorted(hard):
            print(f"  [{kind:9s}] {ident:14s} {str(label)[:24]:26s} "
                  f"{field:12s} {str(value):14s} {problem}")

    if "--tsv" in sys.argv:
        out = os.path.join(HERE, "..", "extraction_v2", "qa", "date_issues.tsv")
        out = os.path.abspath(out)
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh, delimiter="\t")
            w.writerow(["kind", "where", "id", "label", "field", "value", "problem"])
            w.writerows(sorted(issues))
        print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
