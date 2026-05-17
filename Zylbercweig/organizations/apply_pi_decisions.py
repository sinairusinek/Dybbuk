"""Apply PI decisions from PI_DECISIONS_COMPANION.md to current data.

Reads each mapping TSV + its source data TSV, applies the rule-set:
  - Judenrats (review_reason=pi_dilemma:judenrat) → Judenrat
  - Maccabi (review_reason=pi_dilemma:sports_zionist_youth_triple_identity) → Sports/Recreation
  - Lodges/fraternal (review_reason=pi_dilemma:fraternal_welfare_or_other) → Fraternal order
    EXCEPT Grand Street Boys → Education
  - core_db row 493 (São Paulo) → Jewish political bodies
  - 5 remaining missing_canonical rows (Event/Forum, Historical movement, Cultural/Intellectual Society,
    Commemorative Org, Research Project/Expedition) → OTHER - elaborate!
  - 2 Sports/Recreation-suggested rows → Sports/Recreation
  - All context_weak rows defaulted to Theatre-related Society/Union via society/club/association/organization
    tag → Trade Union / Professional Association

For every change: update mapping TSV's canonical_type, the source data TSV's type column,
set decided_via=pi_override, update review_reason with the source PI decision.
"""
from __future__ import annotations
import csv
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

# Canonical strings (must match map_canonical_types_v3.py)
JUDENRAT = "Judenrat"
SPORTS = "Sports/Recreation"
FRATERNAL = "Fraternal order"
TRADE_UNION = "Trade Union / Professional Association"
JEW_POL = "Jewish political bodies"
EDUCATION = "Education"
OTHER = "OTHER - elaborate!"

SPECS = [
	{
		"map": HERE / "organizations_clustered_canonical_mapping.tsv",
		"data": HERE / "organizations_clustered.tsv",
		"type_col": "_ - organizations - _ - org_type",
	},
	{
		"map": HERE / "org_alignment_review_canonical_mapping.tsv",
		"data": HERE / "org_alignment_review.tsv",
		"type_col": "org_type",
	},
	{
		"map": HERE / "org_addresses_review_canonical_mapping.tsv",
		"data": HERE / "org_addresses_review.tsv",
		"type_col": "org_type",
	},
	{
		"map": HERE / "core_db_canonical_mapping.tsv",
		"data": HERE / "core_db.tsv",
		"type_col": "org_type",
	},
]

GENERIC_TAGS_FOR_TRADE_UNION_REDEFAULT = {
	"society", "club", "association", "organization", "Organization",
	"געזעלשאַפט", "ארגאניזאציע", "org", "other",
	"social organization", "philanthropic society", "welfare organization",
	"relief society", "social_relief_org", "cultural society", "culture society",
	"cultural_organization", "cultural department", "cultural_center",
	"קולטור-געזעלשאַפט", "kultur-gezelshaft",
	"community", "community_org", "community_center", "community organization",
	"communal_organization", "charity",
}

# Suggested-new-canonical strings that should now route to specific canonicals
SPORTS_SUGGEST = {"sports/recreation", "sports / recreation", "sports"}
OTHER_REROUTE_SUGGESTS = {
	"event series / forum", "event/festival",
	"historical movement", "cultural/intellectual movement",
	"cultural/intellectual society", "commemorative organization",
	"research project/expedition", "movement",
}


def is_grand_street_boys(name: str) -> bool:
	n = name.lower()
	return "grand street boys" in n or "גרענד סטריט-באָיס" in n or "גרענד סטריט באָיס" in n


def main() -> None:
	totals = {
		"judenrat": 0, "sports_maccabi": 0, "sports_suggested": 0,
		"fraternal": 0, "grand_street_boys": 0,
		"sao_paulo": 0, "other_reroute": 0, "trade_union_default": 0,
	}

	for spec in SPECS:
		map_path = spec["map"]
		data_path = spec["data"]
		type_col = spec["type_col"]
		if not map_path.exists() or not data_path.exists():
			print(f"skip (missing): {map_path.name}")
			continue

		with map_path.open(newline="", encoding="utf-8") as f:
			rdr = csv.DictReader(f, delimiter="\t")
			map_fields = list(rdr.fieldnames or [])
			map_rows = list(rdr)
		with data_path.open(newline="", encoding="utf-8") as f:
			rdr = csv.DictReader(f, delimiter="\t")
			data_fields = list(rdr.fieldnames or [])
			data_rows = list(rdr)

		# Positional alignment between map and data rows (this is how v3 wrote them)
		for i, mrow in enumerate(map_rows):
			drow = data_rows[i] if i < len(data_rows) else None
			name = mrow.get("name", "") or (drow.get("clustered organization", "") if drow else "") or (drow.get("canonical_yiddish", "") if drow else "") or (drow.get("name", "") if drow else "")
			review_reason = mrow.get("review_reason", "")
			current_canon = mrow.get("canonical_type", "")
			db_id = mrow.get("row_id", "")
			decided_via = mrow.get("decided_via", "")
			llm_sugg = (mrow.get("llm_suggested_new_canonical") or "").strip().lower()

			new_canon = None
			new_reason = None
			tag_for_total = None

			# 1) Judenrat
			if review_reason == "pi_dilemma:judenrat":
				new_canon = JUDENRAT
				new_reason = "pi_decision:judenrat_own_type"
				tag_for_total = "judenrat"

			# 2) Maccabi-class
			elif review_reason == "pi_dilemma:sports_zionist_youth_triple_identity":
				new_canon = SPORTS
				new_reason = "pi_decision:sports_recreation_own_type"
				tag_for_total = "sports_maccabi"

			# 3) Fraternal (excluding Grand Street Boys)
			elif review_reason == "pi_dilemma:fraternal_welfare_or_other":
				if is_grand_street_boys(name):
					new_canon = EDUCATION
					new_reason = "pi_decision:grand_street_boys_education"
					tag_for_total = "grand_street_boys"
				else:
					new_canon = FRATERNAL
					new_reason = "pi_decision:fraternal_order_own_type"
					tag_for_total = "fraternal"

			# 4) Specific named rows
			elif review_reason == "pi_dilemma:sao_paulo_yiddish_society_scope":
				new_canon = JEW_POL
				new_reason = "pi_decision:sao_paulo_jewish_political"
				tag_for_total = "sao_paulo"

			# 5) LLM-suggested rerouting
			elif llm_sugg:
				if any(s in llm_sugg for s in SPORTS_SUGGEST):
					new_canon = SPORTS
					new_reason = "pi_decision:sports_recreation_own_type"
					tag_for_total = "sports_suggested"
				elif any(s in llm_sugg for s in OTHER_REROUTE_SUGGESTS):
					new_canon = OTHER
					new_reason = "pi_decision:remaining_missing_to_other"
					tag_for_total = "other_reroute"

			# 6) Default re-route: context_weak rows currently in Theatre-related Society/Union
			#    via society/club/association/organization tag → Trade Union / Professional Association
			elif (decided_via == "context_weak"
				  and current_canon == "Theatre-related Society/ Union"
				  and mrow.get("original_type", "") in GENERIC_TAGS_FOR_TRADE_UNION_REDEFAULT):
				new_canon = TRADE_UNION
				new_reason = "pi_decision:society_default_trade_union"
				tag_for_total = "trade_union_default"

			if new_canon and new_canon != current_canon:
				mrow["canonical_type"] = new_canon
				if drow is not None:
					drow[type_col] = new_canon
				mrow["decided_via"] = "pi_override"
				mrow["review_reason"] = new_reason
				mrow["changed"] = "yes"
				# These have been resolved by PI decision
				mrow["needs_review"] = ""
				totals[tag_for_total] += 1

		# Write back
		with map_path.open("w", newline="", encoding="utf-8") as f:
			w = csv.DictWriter(f, fieldnames=map_fields, delimiter="\t")
			w.writeheader()
			for r in map_rows:
				w.writerow({k: r.get(k, "") for k in map_fields})
		with data_path.open("w", newline="", encoding="utf-8") as f:
			w = csv.DictWriter(f, fieldnames=data_fields, delimiter="\t")
			w.writeheader()
			for r in data_rows:
				w.writerow({k: r.get(k, "") for k in data_fields})
		print(f"updated {map_path.name}")

	print("\n=== Per-category change counts (all files combined) ===")
	for k, v in totals.items():
		print(f"  {v:5d}  {k}")
	print(f"  -----")
	print(f"  {sum(totals.values()):5d}  total")


if __name__ == "__main__":
	main()
