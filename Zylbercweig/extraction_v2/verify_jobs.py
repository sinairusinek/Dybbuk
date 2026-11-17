"""Gemini verification pass over three v2 outputs (cheap judge, no Claude).

  c_org   every job-C organization mapped to core_db/cluster: same organization?
  a_kin   job-A kin_of_subject full forms (surname inferred): is the mention the
          subject's relative as claimed?
  b_src   job-B "Source" relations: is the person really a source about the subject?

  GOOGLE_API_KEY=... python3.11 verify_jobs.py all
Writes verify_<job>.jsonl; build_er_context.py reads them (a FALSE verdict
drops the c_org_id / in-entry full form; b_src verdicts become a column).
"""
import csv, sys
from pathlib import Path

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import gemini_batch

csv.field_size_limit(10**9)
ER = HERE.parent / "datasets" / "surname_er" / "er_mention_context.tsv"

COMMON = ("You check one claim per item about a person mention in Zalmen Zylbercweig's Lexicon of the "
          "Yiddish Theatre (Yiddish). Judge ONLY from the text given. Return a JSON array, one object per "
          "item, same order: {\"id\": ..., \"ok\": true|false, \"why\": \"<=15 words\"}.\n")
PROMPTS = {
    "c_org": COMMON + "Claim: the organization phrase in the sentence (org_in_sentence) and the curated "
             "organization name (matched_org) are the SAME organization (same troupe/theatre; a possessive or "
             "spelling variant is fine; a different person's troupe or a different theatre is NOT).",
    "a_kin": COMMON + "Claim: the mention is a relative of the entry's subject as the evidence says, and "
             "full_form is a reasonable name for them (first name from the text + the family surname). "
             "False if the mention is not a relative, or the evidence does not concern this mention.",
    "b_src": COMMON + "Claim: the mentioned person is a SOURCE about the entry's subject (wrote about them, "
             "reviewed them, was an informant, or is quoted in the entry), as type says. False if the "
             "evidence shows a different tie (e.g. colleague, relative) or no tie.",
}


def items():
    er = list(csv.DictReader(open(ER, encoding="utf-8"), delimiter="\t"))
    c = [{"id": r["mention_id"], "sentence": r["relation_sentence"][:400],
          "org_in_sentence": r["c_org_surface"], "matched_org": r["c_org_name"]}
         for r in er if r["c_org_id"]]
    A = list(csv.DictReader(open(HERE / "a_full_form.tsv", encoding="utf-8"), delimiter="\t"))
    a = [{"id": r["mention_id"], "entry_subject": r["host_heading"], "mention": r["mention_name"],
          "full_form": r["full_form"], "evidence": r["evidence"]}
         for r in A if r["method"] == "llm:kin_of_subject" and r["full_form"]]
    B = list(csv.DictReader(open(HERE / "b_recategorized.tsv", encoding="utf-8"), delimiter="\t"))
    b = [{"id": r["mention_id"], "entry_subject": r["host_heading"], "mention": r["mention_name"],
          "type": r["type"], "evidence": r["evidence"]}
         for r in B if r["category"] == "Source"]
    return {"c_org": c, "a_kin": a, "b_src": b}


if __name__ == "__main__":
    want = sys.argv[1] if len(sys.argv) > 1 else "all"
    for job, its in items().items():
        if want in ("all", job):
            gemini_batch.run(its, PROMPTS[job], HERE / f"verify_{job}.jsonl", batch_size=30, workers=10)
