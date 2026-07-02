"""LLM-drafted people alignment decisions (analog of orgs/llm_draft_alignment.py).

Reads undecided rows from people_alignment_queue.tsv and asks Gemini 3 Flash to
draft a decision per subject-entry. Output → people_alignment_drafts.tsv;
advisory only, never touches the queue itself. Resumable.

Differences from the org drafter:
  - Decision vocabulary is people-specific:
      ALIGN     — entry matches a candidate DB row
      NEW       — entry is a real person not in the DB
      MERGE     — entry is the same person as another *subject-entry* across
                  volumes (this is the dedup case — the LLM points to the other
                  xml_id rather than a db_id)
      PSEUDONYM — entry is an alias for a person who has another entry under
                  their real name (typed separately from MERGE because the
                  RA workflow differs)
      DISAMBIG  — name is shared by multiple known people; needs PI
      DEFER     — insufficient information
  - Few-shot examples are drawn from people_alignment_review.tsv (RA decisions
    + Duplication Check judgments).
  - The prompt includes the candidate DB rows AND the top dedup candidates from
    people_dedup_candidates.tsv (so the LLM can choose MERGE when the right
    answer is "this is the same person as P-1-facs_XXX").

This file is the scaffolding. It does NOT run the model — invocation costs
real money and we want the user to verify the few-shot pool + prompt first.
Run with --dry-run to print the prompt for the first N rows without calling
the API. Run with --execute to actually call the API.

Usage:
    python llm_draft_alignment.py --dry-run --limit 3
    GOOGLE_API_KEY=... python llm_draft_alignment.py --execute --limit 50 --stratified
"""
from __future__ import annotations
import argparse, csv, json, os, random, sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).parent
QUEUE_TSV = HERE / "people_alignment_queue.tsv"
DB_TSV = HERE / "people_db.tsv"
REVIEW_TSV = HERE / "people_alignment_review.tsv"
DEDUP_TSV = HERE / "people_dedup_candidates.tsv"
PEOPLE_TSV = HERE / "people_extracted.tsv"
OUT_TSV = HERE / "people_alignment_drafts.tsv"

DEFAULT_MODEL = "gemini-3-flash-preview"
DECISION_VOCAB = ["ALIGN", "NEW", "MERGE", "PSEUDONYM", "DISAMBIG", "DEFER"]

SYSTEM_TEMPLATE = """You draft alignment decisions for the Zylbercweig Yiddish theatre \
lexicon people-matching pipeline. Native-grade reading in Yiddish, Hebrew, and English.

For each subject-entry you receive:
- volume + xml_id + heading (surname, given) from the Zylbercweig Lexicon
- names_variants — all surface forms (pseudonyms, initials, English forms)
- entry_type (actor / writer / musician / …)
- birth_year, death_year
- birth_place, death_place
- top-5 candidate DB rows from the "DiJeSt / Zylbercweig people" external DB \
(db_id, hebname, english) ranked by trigram + IPA cross-script similarity
- top-3 dedup candidates from OTHER volumes' subject-entries (xml_id, heading, \
volume, birth_year, death_year, composite score)

Decide which of these decisions applies and reply with strict single-line JSON:

{{"draft_decision": "<one of: {vocab}>", \
"draft_aligned_db_id": "<db_id or empty>", \
"draft_merge_xml_id": "<xml_id of the OTHER entry, or empty>", \
"confidence": "<high|medium|low>", \
"rationale": "<<=25 words citing the signals>"}}

Decision vocabulary:
- ALIGN: entry matches one of the candidate DB rows; fill draft_aligned_db_id. \
Require name + (date OR place) agreement (or no contradiction) for high confidence.
- NEW: entry is a real person not in the DB AND not a duplicate of an existing \
subject-entry.
- MERGE: entry is the same person as another subject-entry in another volume \
(Zylbercweig wrote them up twice); fill draft_merge_xml_id. Require name + \
dates to agree, OR same alias in brackets, OR explicit cross-reference.
- PSEUDONYM: entry is an alias for a person whose real name has its own entry \
elsewhere; treat as MERGE-like but flag the alias relationship in rationale.
- DISAMBIG: bare-surname or initial-only name shared by multiple people; cannot \
decide without context.
- DEFER: insufficient info; come back later.

Confidence rubric:
- high: name + (dates OR place OR pseudonym alias) all consistent; one strong \
positive signal AND no contradicting signal.
- medium: one signal is missing or partly disagrees (e.g. one birth year off by 2).
- low: signals conflict, name is sparse, or candidates look weak.

Below are real RA decisions from this dataset to anchor your style. Match their \
reasoning approach.

=========== EXAMPLES ===========
{examples}
=========== END EXAMPLES ===========
"""


def load_tsv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t"))


def fmt_db_candidates(row: dict, db_index: dict[str, dict]) -> str:
    ids = [x.strip() for x in (row.get("candidate_db_ids") or "").split("|") if x.strip()]
    scores = [x.strip() for x in (row.get("candidate_scores") or "").split("|")]
    if not ids:
        return "  (no DB candidates)"
    lines = []
    for i, db_id in enumerate(ids[:5]):
        db = db_index.get(db_id, {})
        score = scores[i] if i < len(scores) else ""
        lines.append(
            f"  - db_id={db_id} score={score} hebname={db.get('hebname', '')} english={db.get('english', '')}"
        )
    return "\n".join(lines)


def fmt_dedup_candidates(xml_id: str, dedup_idx: dict[str, list]) -> str:
    items = dedup_idx.get(xml_id, [])[:3]
    if not items:
        return "  (no dedup candidates)"
    return "\n".join(
        f"  - xml_id={d['other_xml']} vol={d['other_vol']} score={d['score']} "
        f"heading={d['other_heading']} birth={d['other_birth']} death={d['other_death']}"
        for d in items
    )


def fmt_entry(row: dict, db_index, dedup_idx) -> str:
    return (
        f"person_id: {row.get('person_id', '')}\n"
        f"xml_id: {row.get('xml_id', '')}\n"
        f"volume: {row.get('volume', '')}\n"
        f"heading: {row.get('heading', '')}\n"
        f"names_variants: {row.get('names_variants', '')}\n"
        f"entry_type: {row.get('entry_type', '')}\n"
        f"birth_year: {row.get('birth_year', '')}    death_year: {row.get('death_year', '')}\n"
        f"db_candidates:\n{fmt_db_candidates(row, db_index)}\n"
        f"dedup_candidates:\n{fmt_dedup_candidates(row.get('xml_id', ''), dedup_idx)}"
    )


def build_dedup_index() -> dict[str, list]:
    """xml_id -> list of dicts (other_xml, other_vol, other_heading, score, ...)
    sorted by composite_score descending."""
    idx: dict[str, list] = defaultdict(list)
    if not DEDUP_TSV.exists():
        return idx
    for r in load_tsv(DEDUP_TSV):
        score = float(r.get("composite_score", "0") or 0)
        for side, other in (("a", "b"), ("b", "a")):
            xml = r.get(f"{side}_xml_id")
            if not xml:
                continue
            idx[xml].append({
                "other_xml": r.get(f"{other}_xml_id", ""),
                "other_vol": r.get(f"{other}_volume", ""),
                "other_heading": r.get(f"{other}_heading", ""),
                "other_birth": r.get(f"{other}_birth", ""),
                "other_death": r.get(f"{other}_death", ""),
                "score": score,
            })
    for xml in idx:
        idx[xml].sort(key=lambda d: -d["score"])
    return idx


CURATED_JSON = HERE / "few_shot_curated.json"


def pick_few_shot_curated(db_index, dedup_idx) -> str:
    """Render hand-curated few-shot examples (few_shot_curated.json).

    Each item names a real queue row by xml_id; the INPUT block is rendered
    from the live queue so the LLM sees exactly the format it will receive,
    while the OUTPUT (decision + hand-written rationale) is curated. Falls
    back to the automatic RA-decision pool when the file is absent.
    """
    curated = json.loads(CURATED_JSON.read_text(encoding="utf-8"))
    queue = load_tsv(QUEUE_TSV)
    by_xml = {r["xml_id"]: r for r in queue}
    out = []
    for ex in curated:
        q = by_xml.get(ex["xml_id"])
        if not q:
            print(f"  [few-shot] WARNING: curated xml_id {ex['xml_id']} not in queue — skipped", file=sys.stderr)
            continue
        ex_json = {k: ex.get(k, "") for k in (
            "draft_decision", "draft_aligned_db_id", "draft_merge_xml_id",
            "confidence", "rationale")}
        out.append(f"INPUT:\n{fmt_entry(q, db_index, dedup_idx)}\nOUTPUT: {json.dumps(ex_json, ensure_ascii=False)}")
    return "\n\n".join(out)


def pick_few_shot(db_index, dedup_idx, max_per_type: int = 2) -> str:
    """Draw real decided rows from the imported RA review TSV."""
    if CURATED_JSON.exists():
        curated = pick_few_shot_curated(db_index, dedup_idx)
        if curated:
            return curated
    review = load_tsv(REVIEW_TSV)
    by_decision = defaultdict(list)
    # Duplication Check rows with same_person=Same → MERGE example
    for r in review:
        if r.get("source_sheet") == "Duplication Check":
            sp = (r.get("same_person") or "").lower()
            if sp.startswith("same"):
                by_decision["MERGE"].append(r)
            elif sp.startswith("not") or "שונה" in (r.get("same_person") or ""):
                by_decision["DISAMBIG"].append(r)
        else:
            if r.get("db_id"):
                by_decision["ALIGN"].append(r)
    picks = []
    for d in DECISION_VOCAB:
        picks.extend(by_decision.get(d, [])[:max_per_type])
    picks = picks[:6]
    out = []
    queue = load_tsv(QUEUE_TSV)
    by_xml = {r["xml_id"]: r for r in queue}
    for r in picks:
        q = by_xml.get(r.get("xml_id"))
        if not q:
            continue
        # Skip ALIGN examples where the gold db_id isn't in our candidate list —
        # teaching the LLM to "ALIGN to 1171" when 1171 isn't in candidates is
        # actively harmful. Better few-shot pool comes from rows where both the
        # gold answer and the candidate list overlap.
        if r.get("source_sheet") != "Duplication Check":
            cand_ids_set = set((q.get("candidate_db_ids") or "").split("|"))
            if r.get("db_id") and r.get("db_id") not in cand_ids_set:
                continue
        is_merge = r.get("source_sheet") == "Duplication Check"
        # for MERGE examples, point the LLM at the OTHER xml_id sharing this db_id
        merge_xml = ""
        if is_merge and r.get("db_id"):
            siblings = [
                rr for rr in load_tsv(REVIEW_TSV)
                if rr.get("source_sheet") == "Duplication Check"
                and rr.get("db_id") == r.get("db_id")
                and rr.get("xml_id") != r.get("xml_id")
            ]
            if siblings:
                merge_xml = siblings[0].get("xml_id", "")
        ex_json = {
            "draft_decision": "MERGE" if is_merge else "ALIGN",
            "draft_aligned_db_id": "" if is_merge else (r.get("db_id", "") or ""),
            "draft_merge_xml_id": merge_xml,
            "confidence": "high",
            "rationale": (r.get("notes") or
                          ("RA marked same person across volumes" if is_merge else "RA aligned to DB row")
                          )[:120],
        }
        out.append(f"INPUT:\n{fmt_entry(q, db_index, dedup_idx)}\nOUTPUT: {json.dumps(ex_json, ensure_ascii=False)}")
    return "\n\n".join(out) or "(no few-shot examples available)"


def parse_json_loose(text: str) -> dict:
    t = text.strip()
    if t.startswith("```"):
        lines = t.split("\n")
        t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
    i, j = t.find("{"), t.rfind("}")
    if i >= 0 and j > i:
        t = t[i:j + 1]
    try:
        return json.loads(t)
    except Exception:
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="print prompt for the first N rows; do NOT call the API")
    ap.add_argument("--execute", action="store_true", help="actually call the API")
    ap.add_argument("--limit", type=int, default=5)
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--out", default=str(OUT_TSV))
    args = ap.parse_args()
    if not args.dry_run and not args.execute:
        sys.exit("specify --dry-run or --execute")

    queue = load_tsv(QUEUE_TSV)
    if not queue:
        sys.exit(f"missing {QUEUE_TSV} — run prepare_alignment.py first")
    db_index = {r["db_id"]: r for r in load_tsv(DB_TSV)}
    dedup_idx = build_dedup_index()

    undecided = [r for r in queue if not (r.get("decision") or "").strip()]
    rows = undecided[: args.limit] if args.limit else undecided
    print(f"queue: {len(queue)} total, {len(undecided)} undecided, processing {len(rows)}")

    examples_block = pick_few_shot(db_index, dedup_idx)
    system_prompt = SYSTEM_TEMPLATE.format(vocab="|".join(DECISION_VOCAB), examples=examples_block)

    if args.dry_run:
        print("=" * 70)
        print("SYSTEM PROMPT")
        print("=" * 70)
        print(system_prompt[:4000])
        print("...")
        for i, r in enumerate(rows):
            print(f"\n--- DRY-RUN INPUT {i+1}/{len(rows)} ---")
            print(fmt_entry(r, db_index, dedup_idx))
        return

    # execute path
    from google import genai
    from google.genai import types
    api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not api_key:
        sys.exit("set GOOGLE_API_KEY or GEMINI_API_KEY")
    os.environ["GOOGLE_API_KEY"] = api_key
    client = genai.Client()
    out_path = Path(args.out)
    # resume
    done = set()
    if out_path.exists():
        for r in load_tsv(out_path):
            done.add(r["xml_id"])

    drafts = []
    if out_path.exists():
        drafts = load_tsv(out_path)

    for i, r in enumerate(rows):
        if r["xml_id"] in done:
            continue
        prompt = f"INPUT:\n{fmt_entry(r, db_index, dedup_idx)}\nOUTPUT:"
        try:
            resp = client.models.generate_content(
                model=args.model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    thinking_config=types.ThinkingConfig(thinking_budget=0),
                    max_output_tokens=2048,
                ),
            )
            text = resp.text or ""
        except Exception as ex:
            text = json.dumps({"error": str(ex)})
        parsed = parse_json_loose(text)
        drafts.append({
            "xml_id": r["xml_id"],
            "person_id": r["person_id"],
            "heading": r["heading"],
            "draft_decision": parsed.get("draft_decision", ""),
            "draft_aligned_db_id": parsed.get("draft_aligned_db_id", ""),
            "draft_merge_xml_id": parsed.get("draft_merge_xml_id", ""),
            "confidence": parsed.get("confidence", ""),
            "rationale": parsed.get("rationale", ""),
            "raw": text.replace("\t", " ").replace("\n", " ")[:1500],
        })
        if (i + 1) % 25 == 0:
            print(f"  drafted {i+1}/{len(rows)}")

    cols = ["xml_id", "person_id", "heading", "draft_decision", "draft_aligned_db_id",
            "draft_merge_xml_id", "confidence", "rationale", "raw"]
    with open(out_path, "w", encoding="utf-8", newline="") as fp:
        w = csv.DictWriter(fp, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        w.writerows(drafts)
    print(f"wrote {out_path} ({len(drafts)} rows)")


if __name__ == "__main__":
    main()
