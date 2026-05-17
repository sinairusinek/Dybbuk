"""Deep LLM review with Opus 4.7.

Reviews v3-mapper output by sending Opus a stratified sample of rows with much
richer context than the shallow Sonnet pass: full sentence, entry heading
(the person), role title, specific relation, original LLM tag, auto canonical,
and the surrounding decision metadata. Opus is asked not just to confirm or
correct but also to flag *systematic* concerns about the auto rule that
produced the decision.

Outputs:
  llm_deep_review.tsv — per-row Opus verdict + a `concern_theme` tag.
  llm_deep_review_summary.tsv — pivot of (auto_canonical × llm_canonical × concern_theme).
"""
from __future__ import annotations
import csv
import json
import os
import random
import sys
from collections import Counter, defaultdict
from pathlib import Path

import anthropic

csv.field_size_limit(sys.maxsize)
random.seed(20260512)

HERE = Path(__file__).parent
MENTIONS = HERE / "organizations_clustered.tsv"
MAPPING = HERE / "organizations_clustered_canonical_mapping.tsv"
POLICY = HERE / "CLASSIFICATION_POLICY.md"
OUT = HERE / "llm_deep_review.tsv"
OUT_SUMMARY = HERE / "llm_deep_review_summary.tsv"

MODEL = "claude-opus-4-5"  # Opus 4.7 alias on the API

# Sample composition
SAMPLES_BY_BUCKET = {
	"Theatre": 15,
	"Traveling Company": 15,
	"Theatre-related Society/ Union": 25,
	"Labour (factory/workshop)": 15,
	"Business": 25,
	"Jewish political bodies": 25,
	"Non-Jewish political bodies": 15,
	"Welfare/Aid organization": 25,
	"Musical organization": 10,
	"Religious institutions/organizations": 10,
	"Media (Radio/ Film/TV)": 10,
	"Heritage Institution": 10,
	"Education": 10,
	"Theatre education": 5,
	"Amateur": 5,
	"Military": 5,
	"Health institutions": 5,
	"Not an organization": 5,
	"OTHER - elaborate!": 10,
	"Kleinkunst": 3,
	"Circus": 3,
	"Library": 3,
}

# Always-include named-entity PI dilemma rows (sample N rows that hit each)
DILEMMA_INCLUDE = {
	"pi_dilemma:fraternal_political_dual_identity": 10,
	"pi_dilemma:judenrat": 5,
	"pi_dilemma:zionist_welfare_dual_identity": 10,
	"pi_dilemma:sports_zionist_youth_triple_identity": 5,
	"pi_dilemma:fraternal_welfare_or_other": 10,
	"pi_dilemma:brewery_relation_conflict": 5,
}

# Decision sources we want to validate (shallow keyword decisions)
TARGET_DECIDED_VIA = {"context", "context_weak", "named_entity"}


def load_policy_excerpt() -> str:
	"""Return the policy's canonical-type definitions to embed in the prompt."""
	if POLICY.exists():
		return POLICY.read_text()
	return ""


SYSTEM_TEMPLATE = """You are an expert reviewer of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization classifications. You have native-grade reading in Yiddish, Hebrew, and English, plus solid historical knowledge of Jewish, Eastern European, and theatre history (1880s–1960s).

The fixed canonical typology and decision policy is below. Internalize it before reviewing.

=========== POLICY ===========
{policy}
=========== END POLICY ===========

For each row I send you, you receive richer context than a tag-keyword rule sees:
- Entry heading (the person whose lexicon entry contains the mention)
- Original LLM tag (free-text type extracted from source)
- Auto-assigned canonical (the v3 mapper's choice)
- Decision metadata (decided_via, review_reason)
- Role title (person's role at this org)
- Specific relation (more detail than category)
- Full sentence

Your job:
1. Decide the correct canonical from the 27 types in the policy.
2. Report whether you agree with the auto-assigned canonical.
3. Tag your finding with ONE concern_theme (see list below) so we can aggregate.
4. Give a concise reason (<=30 words).

concern_theme codes (pick exactly one):
- "agree" — you confirm the auto choice.
- "shallow_keyword_overfire" — auto rule fired on a keyword that doesn't fit this specific entity (e.g., "arbeter" matched but entity is a fraternal society, not a workplace).
- "shallow_keyword_missed_cue" — context clearly indicates a different bucket but auto rule didn't catch the cue.
- "wrong_named_entity" — auto's named-entity match is wrong for this entity (false positive).
- "policy_ambiguous" — the entity is genuinely ambiguous; the policy doesn't unambiguously direct.
- "missing_canonical_type" — entity doesn't fit any of the 27 types; we may need a new canonical.
- "pi_judgement_needed" — entity is a genuine dual/multi-identity case worth flagging to the PI (e.g., Zionist+welfare).
- "data_quality" — the row's name/sentence is malformed or wrong (LLM extraction error upstream).
- "minor_drift" — auto picked an acceptable bucket but a different one is slightly better (low-priority disagreement).

Reply with strict JSON on ONE line, no code fences:
{{"canonical": "<exact type string>", "agree": true|false, "concern_theme": "<code>", "reason": "<<=30 words>", "suggested_new_canonical": "<name>"}}

`suggested_new_canonical` is only filled when concern_theme is "missing_canonical_type"; otherwise empty string.

Use EXACTLY these strings for canonical (case and punctuation matter):
Theatre, Traveling Company, Company on Tour, Amateur, Kleinkunst, Circus, Theatre education, Publisher, Printer, Printer/Publisher, Journals/ Newspapers, Media (Radio/ Film/TV), Library, Heritage Institution, Education, Musical organization, Theatre-related Society/ Union, Religious institutions/organizations, Jewish political bodies, Non-Jewish political bodies, Welfare/Aid organization, Business, Labour (factory/workshop), Health institutions, Military, Not an organization, OTHER - elaborate!"""


def load_sample() -> list[dict]:
	# Build (per row from mapping + sentence/heading/role from mentions, joined positionally)
	by_bucket: dict[str, list[dict]] = defaultdict(list)
	by_dilemma: dict[str, list[dict]] = defaultdict(list)

	mention_cols = {
		"name": "clustered organization",
		"sentence": "_ - organizations - _ - relations - _ - original_sentence",
		"role": "_ - organizations - _ - relations - _ - role_title",
		"specific_rel": "_ - organizations - _ - relations - _ - specific_relation",
		"rel_cat": "_ - organizations - _ - relations - _ - category",
		"heading": "_ - heading",
		"sub": "_ - subheading",
	}

	with MENTIONS.open(newline="", encoding="utf-8") as fm, \
		 MAPPING.open(newline="", encoding="utf-8") as fp:
		mreader = csv.DictReader(fm, delimiter="\t")
		preader = csv.DictReader(fp, delimiter="\t")
		for mrow, prow in zip(mreader, preader):
			decided_via = prow.get("decided_via", "")
			if decided_via not in TARGET_DECIDED_VIA:
				continue
			canon = prow.get("canonical_type", "")
			row = {
				"row_id": prow.get("row_id", ""),
				"name": prow.get("name", "") or mrow.get(mention_cols["name"], ""),
				"original_type": prow.get("original_type", ""),
				"auto_canonical": canon,
				"decided_via": decided_via,
				"review_reason": prow.get("review_reason", ""),
				"heading": mrow.get(mention_cols["heading"], ""),
				"subheading": mrow.get(mention_cols["sub"], ""),
				"role": mrow.get(mention_cols["role"], ""),
				"specific_relation": mrow.get(mention_cols["specific_rel"], ""),
				"relation_category": mrow.get(mention_cols["rel_cat"], ""),
				"sentence": mrow.get(mention_cols["sentence"], ""),
			}
			by_bucket[canon].append(row)
			rr = row["review_reason"]
			if rr and rr.startswith("pi_dilemma:"):
				by_dilemma[rr].append(row)

	picks: list[dict] = []
	seen_ids: set[tuple[str, str]] = set()

	def _add(r: dict) -> None:
		k = (r["row_id"], r["name"])
		if k in seen_ids:
			return
		seen_ids.add(k)
		picks.append(r)

	# Dilemma-first
	for reason, n in DILEMMA_INCLUDE.items():
		rows = list(by_dilemma.get(reason, []))
		random.shuffle(rows)
		for r in rows[:n]:
			_add(r)

	# Then stratified by bucket
	for bucket, n in SAMPLES_BY_BUCKET.items():
		rows = list(by_bucket.get(bucket, []))
		random.shuffle(rows)
		added = 0
		for r in rows:
			if added >= n:
				break
			k = (r["row_id"], r["name"])
			if k in seen_ids:
				continue
			_add(r)
			added += 1

	return picks


def main() -> None:
	if not os.environ.get("ANTHROPIC_API_KEY"):
		sys.exit("ANTHROPIC_API_KEY not set")
	policy = load_policy_excerpt()
	system_prompt = SYSTEM_TEMPLATE.format(policy=policy)
	picks = load_sample()
	print(f"Sample size: {len(picks)} rows")
	print(f"Policy length: {len(policy):,} chars / system prompt ~{len(system_prompt):,} chars")

	client = anthropic.Anthropic()
	results: list[dict] = []
	for i, row in enumerate(picks, 1):
		user_msg = (
			f"Entry heading (person): {row['heading']}\n"
			f"Subheading: {row['subheading']}\n"
			f"Entity name (org): {row['name']}\n"
			f"Original LLM tag: {row['original_type']}\n"
			f"Relation category: {row['relation_category']}\n"
			f"Specific relation: {row['specific_relation']}\n"
			f"Role title: {row['role']}\n"
			f"Sentence: {row['sentence']}\n"
			f"Auto-assigned canonical: {row['auto_canonical']}\n"
			f"Decided via: {row['decided_via']}\n"
			f"Review reason (if any): {row['review_reason']}\n\n"
			f"Reply with strict JSON only."
		)
		try:
			resp = client.messages.create(
				model=MODEL,
				max_tokens=400,
				system=[{"type": "text", "text": system_prompt,
						 "cache_control": {"type": "ephemeral"}}],
				messages=[{"role": "user", "content": user_msg}],
			)
		except Exception as e:
			print(f"  row {i}: API error: {e}")
			continue
		text = resp.content[0].text.strip()
		# Strip code fences if present
		t = text
		if t.startswith("```"):
			lines = t.split("\n")
			t = "\n".join(lines[1:-1]) if len(lines) >= 3 else t
		try:
			data = json.loads(t)
		except Exception:
			data = {"canonical": "", "agree": None, "concern_theme": "parse_err",
					"reason": text[:200], "suggested_new_canonical": ""}

		results.append({
			**row,
			"llm_canonical": (data.get("canonical") or "").strip(),
			"llm_agree": "yes" if data.get("agree") is True else (
				"no" if data.get("agree") is False else ""),
			"concern_theme": data.get("concern_theme", ""),
			"llm_reason": data.get("reason", ""),
			"suggested_new_canonical": data.get("suggested_new_canonical", ""),
			"input_tokens": resp.usage.input_tokens,
			"cache_read": getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
			"cache_create": getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
			"output_tokens": resp.usage.output_tokens,
		})
		if i % 20 == 0 or i == len(picks):
			print(f"  {i}/{len(picks)} done")

	# Write per-row TSV
	cols = ["row_id", "heading", "name", "original_type", "auto_canonical",
			"decided_via", "review_reason", "relation_category", "role",
			"sentence", "llm_canonical", "llm_agree", "concern_theme",
			"llm_reason", "suggested_new_canonical",
			"input_tokens", "cache_read", "cache_create", "output_tokens"]
	with OUT.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
		w.writeheader()
		for r in results:
			w.writerow({k: r.get(k, "") for k in cols})

	# Aggregate: concern_theme counts; (auto, llm) confusion; suggested_new
	concern_counts = Counter(r["concern_theme"] for r in results)
	confusion = Counter((r["auto_canonical"], r["llm_canonical"],
						 r["concern_theme"]) for r in results)
	suggested = Counter(r["suggested_new_canonical"] for r in results
						 if r["suggested_new_canonical"])

	with OUT_SUMMARY.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["auto_canonical", "llm_canonical", "concern_theme", "n_rows"])
		for (a, l, c), n in confusion.most_common():
			w.writerow([a, l, c, n])

	# Cost
	tot_in = sum(r["input_tokens"] for r in results)
	tot_out = sum(r["output_tokens"] for r in results)
	tot_cache_r = sum(r["cache_read"] for r in results)
	tot_cache_c = sum(r["cache_create"] for r in results)
	# Opus 4.x pricing: $15/MTok input, $75/MTok output, $1.50 cache-read, $18.75 cache-write.
	cost = ((tot_in - tot_cache_r) * 15 + tot_cache_r * 1.50 + tot_cache_c * 18.75) / 1e6 + tot_out * 75 / 1e6

	# Console summary
	print(f"\n=== Concern themes ===")
	for c, n in concern_counts.most_common():
		print(f"  {n:4d}  {c}")
	if suggested:
		print(f"\n=== Suggested new canonical types ===")
		for s, n in suggested.most_common():
			print(f"  {n:4d}  {s!r}")
	print(f"\nTokens: in={tot_in:,} (cache_read={tot_cache_r:,}, cache_create={tot_cache_c:,}) out={tot_out:,}")
	print(f"Estimated cost: ${cost:.2f}")
	print(f"Output: {OUT.name}")


if __name__ == "__main__":
	main()
