"""Map org_type values to canonical types across mentions, clusters, and DB.

Writes:
  - backups (*.pre_canonical_backup)
  - in-place canonical rewrites of:
      organizations_clustered.tsv  (mentions)
      org_alignment_review.tsv     (clusters)
      org_addresses_review.tsv     (db entities working copy)
      core_db.tsv                  (db entities canonical) -- already mapped, re-run safe
  - per-file mapping TSVs: <stem>_canonical_mapping.tsv
"""
from __future__ import annotations
import csv
import shutil
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).parent

# --- Canonical mapping ---
# `REVIEW` set marks judgement-call mappings (flagged needs_review=yes).
REVIEW: set[str] = set()


def _add(src: str, canon: str, review: bool = False) -> tuple[str, str]:
	if review:
		REVIEW.add(src)
	return src, canon


_PAIRS: list[tuple[str, str]] = [
	# Theatre
	("theatre", "Theatre"),
	("Theatre", "Theatre"),
	("טעאַטער", "Theatre"),
	("בינע", "Theatre"),
	("venue", "Theatre"),
	# Traveling Company (touring as core identity)
	("troupe", "Traveling Company"),
	("Troupe", "Traveling Company"),
	("טרופּע", "Traveling Company"),
	("theatre_group", "Traveling Company"),
	("theatre troupe", "Traveling Company"),
	("theatre troupe ", "Traveling Company"),
	("טעאַטער-טרופּע", "Traveling Company"),
	("טעאַטער-קאָלעקטיוו", "Traveling Company"),
	("Traveling Company", "Traveling Company"),
	# Company on Tour (only when explicit)
	("Company on Tour", "Company on Tour"),
	# Company (ambiguous, flag)
	_add("company", "Traveling Company", review=True),
	# Amateur
	("Amateur", "Amateur"),
	("amateur troupe", "Amateur"),
	("drama_circle", "Amateur"),
	("drama club", "Amateur"),
	("drama circle", "Amateur"),
	("union drama circle", "Amateur"),
	("theatre_club", "Amateur"),
	("theatre club", "Amateur"),
	# Theatre education
	("theatre studio", "Theatre education"),
	_add("studio", "Theatre education", review=True),
	_add("סטוּדיע", "Theatre education", review=True),
	_add("academy", "Theatre education", review=True),
	# Theatre-related Society/Union
	("theatre_society", "Society/ Union"),
	("theatre organization", "Society/ Union"),
	("theatre_union", "Society/ Union"),
	("theatre society", "Society/ Union"),
	("theatre_collective", "Society/ Union"),
	("ליטעראַריש-דראַמאַטישע געזעלשאַפט", "Society/ Union"),
	# General Education
	("school", "Education"),
	("School", "Education"),
	("Education", "Education"),
	("university", "Education"),
	("אוניווערזיטעט", "Education"),
	_add("institute", "Education", review=True),
	_add("אינסטיטוט", "Education", review=True),
	("research institute", "Education"),
	("student_organization", "Education"),
	_add("youth_organization", "Education", review=True),
	_add("יוגנט-אָרגאַניזאַצע", "Education", review=True),
	# Musical organization
	("choir", "Musical organization"),
	("כאָר", "Musical organization"),
	("orchestra", "Musical organization"),
	("אָרקעסטער", "Musical organization"),
	("band", "Musical organization"),
	("philharmonic", "Musical organization"),
	# Kleinkunst
	("Kleinkunst", "Kleinkunst"),
	# Identity passthroughs for canonical types
	("Musical organization", "Musical organization"),
	("Society/ Union", "Society/ Union"),
	("Political bodies", "Political bodies"),
	("Religious institutions/organizations", "Religious institutions/organizations"),
	("Business", "Business"),
	("Labour", "Labour"),
	("Health institutions", "Health institutions"),
	("Military", "Military"),
	("Theatre education", "Theatre education"),
	("OTHER - elaborate!", "OTHER - elaborate!"),
	("Not an organization", "Not an organization"),
	# Religious
	("synagogue", "Religious institutions/organizations"),
	("shul", "Religious institutions/organizations"),
	("שול", "Religious institutions/organizations"),
	("temple", "Religious institutions/organizations"),
	("house of prayer", "Religious institutions/organizations"),
	("religious", "Religious institutions/organizations"),
	("religious_institution", "Religious institutions/organizations"),
	("religious_organization", "Religious institutions/organizations"),
	("religious_group", "Religious institutions/organizations"),
	("religious court", "Religious institutions/organizations"),
	("synagogue choir", "Religious institutions/organizations"),
	("קהלה", "Religious institutions/organizations"),
	# Political
	("party", "Political bodies"),
	("פארטיי", "Political bodies"),
	("פּאַרטיי-אָרגאַניזאַציע", "Political bodies"),
	("political_party", "Political bodies"),
	("political organization", "Political bodies"),
	("political movement", "Political bodies"),
	("political_circle", "Political bodies"),
	("movement", "Political bodies"),
	("revolutionary organization", "Political bodies"),
	("party branch", "Political bodies"),
	_add("union branch", "Political bodies", review=True),
	("parliament", "Political bodies"),
	("government", "Political bodies"),
	("government body", "Political bodies"),
	("government_office", "Political bodies"),
	("government_agency", "Political bodies"),
	("government_department", "Political bodies"),
	("government_administration", "Political bodies"),
	("government building", "Political bodies"),
	("state_institution", "Political bodies"),
	("state_government", "Political bodies"),
	("city council", "Political bodies"),
	("council", "Political bodies"),
	("executive committee", "Political bodies"),
	_add("committee", "Political bodies", review=True),
	("קאָמיטעט", "Political bodies"),
	("קאָמיסאַריאָט", "Political bodies"),
	("commission", "Political bodies"),
	("congress", "Political bodies"),
	("conference", "Political bodies"),
	("embassy", "Political bodies"),
	_add("agency", "Political bodies", review=True),
	("court", "Political bodies"),
	_add("department", "Political bodies", review=True),
	("administration", "Political bodies"),
	("division", "Political bodies"),
	_add("office", "Political bodies", review=True),
	("communal office", "Political bodies"),
	("Gmina", "Political bodies"),
	# Military
	("army", "Military"),
	("אַרמיי", "Military"),
	("ארמיי", "Military"),
	("אַרמעע", "Military"),
	("military", "Military"),
	("military organization", "Military"),
	# Media
	("radio", "Media (Radio/ Film/TV)"),
	("tv", "Media (Radio/ Film/TV)"),
	("television", "Media (Radio/ Film/TV)"),
	("radio_station", "Media (Radio/ Film/TV)"),
	("radio program", "Media (Radio/ Film/TV)"),
	("broadcaster", "Media (Radio/ Film/TV)"),
	("media", "Media (Radio/ Film/TV)"),
	("Media (Radio/ Film)", "Media (Radio/ Film/TV)"),
	("Media (Radio/ Film/TV)", "Media (Radio/ Film/TV)"),
	("film", "Media (Radio/ Film/TV)"),
	("film_company", "Media (Radio/ Film/TV)"),
	("film_studio", "Media (Radio/ Film/TV)"),
	("cinema studio", "Media (Radio/ Film/TV)"),
	("film publisher", "Media (Radio/ Film/TV)"),
	("news agency", "Media (Radio/ Film/TV)"),
	# Journals/Newspapers
	("newspaper", "Journals/ Newspapers"),
	("journal", "Journals/ Newspapers"),
	("newspaper/journal", "Journals/ Newspapers"),
	("periodical", "Journals/ Newspapers"),
	("publication", "Journals/ Newspapers"),
	("זשורנאַל", "Journals/ Newspapers"),
	("Journals/ Newspapers", "Journals/ Newspapers"),
	# Publisher
	("publisher", "Publisher"),
	("Publisher", "Publisher"),
	# Printer
	("printer", "Printer"),
	("Printer", "Printer"),
	("Printer/Publisher", "Printer/Publisher"),
	("דרוקעריי", "Printer"),
	# Heritage
	("museum", "Heritage Institution"),
	("מוזיי", "Heritage Institution"),
	("archive", "Heritage Institution"),
	("gallery", "Heritage Institution"),
	("exhibition", "Heritage Institution"),
	("historical_location", "Heritage Institution"),
	("Heritage Institution", "Heritage Institution"),
	# Library
	("library", "Library"),
	("Library", "Library"),
	# Health
	("hospital", "Health institutions"),
	("clinic", "Health institutions"),
	("sanatorium", "Health institutions"),
	("medical institution", "Health institutions"),
	_add("pharmacy", "Business", review=True),
	# Labour
	_add("union", "Labour", review=True),
	("יוניע", "Labour"),
	("פאַריין", "Labour"),
	("Union", "Labour"),
	("labor union", "Labour"),
	("factory", "Labour"),
	("workshop", "Labour"),
	("workplace", "Labour"),
	_add("labor camp", "Labour", review=True),
	("guild", "Labour"),
	_add("union/association", "Labour", review=True),
	# Business
	("business", "Business"),
	("firm", "Business"),
	("shop", "Business"),
	("bank", "Business"),
	("hotel", "Business"),
	("restaurant", "Business"),
	("cafe", "Business"),
	("saloon", "Business"),
	("tavern", "Business"),
	("insurance", "Business"),
	("law_firm", "Business"),
	("brewery", "Business"),
	("farm", "Business"),
	("booking_office", "Business"),
	_add("production", "Business", review=True),
	# Circus
	("circus", "Circus"),
	("Circus", "Circus"),
	# Not an organization
	("ghetto", "Not an organization"),
	("camp", "Not an organization"),
	("concentration camp", "Not an organization"),
	("refugee_camp", "Not an organization"),
	("colony", "Not an organization"),
	("home", "Not an organization"),
	("children's home", "Not an organization"),
	_add("orphanage", "Not an organization", review=True),
	("residence", "Not an organization"),
	("resort", "Not an organization"),
	("book", "Not an organization"),
	("lexicon", "Not an organization"),
	# OTHER (flagged)
	_add("organization", "OTHER - elaborate!", review=True),
	_add("Organization", "Society/ Union", review=True),  # core_db row 493 case
	_add("ארגאניזאציע", "OTHER - elaborate!", review=True),
	_add("society", "OTHER - elaborate!", review=True),
	_add("געזעלשאַפט", "OTHER - elaborate!", review=True),
	_add("club", "OTHER - elaborate!", review=True),
	_add("association", "OTHER - elaborate!", review=True),
	_add("fraternal", "OTHER - elaborate!", review=True),
	_add("lodge", "OTHER - elaborate!", review=True),
	_add("philanthropic society", "OTHER - elaborate!", review=True),
	_add("charity", "OTHER - elaborate!", review=True),
	_add("welfare organization", "OTHER - elaborate!", review=True),
	_add("relief society", "OTHER - elaborate!", review=True),
	_add("social organization", "OTHER - elaborate!", review=True),
	_add("social_relief_org", "OTHER - elaborate!", review=True),
	_add("fund", "OTHER - elaborate!", review=True),
	_add("foundation", "OTHER - elaborate!", review=True),
	_add("community", "OTHER - elaborate!", review=True),
	_add("community_org", "OTHER - elaborate!", review=True),
	_add("community_center", "OTHER - elaborate!", review=True),
	_add("community organization", "OTHER - elaborate!", review=True),
	_add("communal_organization", "OTHER - elaborate!", review=True),
	_add("cultural_organization", "OTHER - elaborate!", review=True),
	_add("cultural society", "OTHER - elaborate!", review=True),
	_add("cultural department", "OTHER - elaborate!", review=True),
	_add("culture society", "OTHER - elaborate!", review=True),
	_add("cultural_center", "OTHER - elaborate!", review=True),
	_add("kultur-gezelshaft", "OTHER - elaborate!", review=True),
	_add("קולטור-געזעלשאַפט", "OTHER - elaborate!", review=True),
	_add("literary group", "OTHER - elaborate!", review=True),
	_add("literary circle", "OTHER - elaborate!", review=True),
	_add("literary collection", "OTHER - elaborate!", review=True),
	_add("kruzhok", "OTHER - elaborate!", review=True),
	_add("קרוזשאָק", "OTHER - elaborate!", review=True),
	_add("miscellany", "OTHER - elaborate!", review=True),
	_add("etc", "OTHER - elaborate!", review=True),
	_add("org", "OTHER - elaborate!", review=True),
	_add("other", "OTHER - elaborate!", review=True),
	_add("institution", "OTHER - elaborate!", review=True),
	_add("center", "OTHER - elaborate!", review=True),
	_add("group", "OTHER - elaborate!", review=True),
	_add("festival", "OTHER - elaborate!", review=True),
	_add("expedition", "OTHER - elaborate!", review=True),
	_add("literary-dramatic society", "Society/ Union", review=True),
	_add("שפּעטער געווען עטלעכע יאָר מענעדזשער פֿון דער יידישער אַקטיאָרן-יוניע", "Society/ Union", review=True),
	# Empty
	("", ""),
]

TAG_MAP: dict[str, str] = dict(_PAIRS)

# Row-id overrides (core_db / org_addresses_review use db_id keys).
ID_OVERRIDES: dict[str, tuple[str, bool]] = {
	# (canonical, needs_review)
	"484": ("Education", True),  # Ostrowski Institute — unclear if theatre institute
	"508": ("Labour", True),     # Vilna Printers' Union — Society/Union restricted to theatre
	"558": ("Musical organization", False),  # Moscow Philharmonic, mis-typed "school"
	"493": ("Society/ Union", True),  # Sao Paulo Yiddish Society — theatre context assumed
}


def remap_value(val: str) -> tuple[str, bool, str]:
	"""Return (new_value, needs_review, status). status is one of:
	  '' (unchanged), 'changed', 'unmapped', 'review'."""
	if val in TAG_MAP:
		new = TAG_MAP[val]
		needs_review = val in REVIEW
		if new != val:
			return new, needs_review, "review" if needs_review else "changed"
		return new, needs_review, "review" if needs_review else ""
	return val, True, "unmapped"


def process_file(
	path: Path,
	type_column: str,
	id_column: str | None = None,
	use_id_overrides: bool = False,
) -> None:
	backup = path.with_suffix(path.suffix + ".pre_canonical_backup")
	if not backup.exists():
		shutil.copy2(path, backup)

	with path.open(newline="", encoding="utf-8") as f:
		reader = csv.DictReader(f, delimiter="\t")
		fieldnames = reader.fieldnames or []
		rows = list(reader)

	mapping_rows: list[list[str]] = []
	unmapped: dict[str, int] = {}
	review_count = 0
	changed_count = 0

	name_col = None
	for cand in ("name", "canonical_yiddish", "clustered organization"):
		if cand in fieldnames:
			name_col = cand
			break

	for row in rows:
		orig = row.get(type_column, "")
		row_id = row.get(id_column, "") if id_column else ""

		needs_review = False
		if use_id_overrides and row_id in ID_OVERRIDES:
			new, needs_review = ID_OVERRIDES[row_id]
			status = "review" if needs_review else ("changed" if new != orig else "")
		else:
			new, needs_review, status = remap_value(orig)

		row[type_column] = new
		if status == "unmapped":
			unmapped[orig] = unmapped.get(orig, 0) + 1
		if needs_review:
			review_count += 1
		if new != orig:
			changed_count += 1

		mapping_rows.append([
			row_id,
			row.get(name_col, "") if name_col else "",
			orig,
			new,
			"yes" if new != orig else "",
			"yes" if needs_review else "",
		])

	with path.open("w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
		writer.writeheader()
		writer.writerows(rows)

	map_out = path.with_name(path.stem + "_canonical_mapping.tsv")
	with map_out.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["row_id", "name", "original_type", "canonical_type", "changed", "needs_review"])
		w.writerows(mapping_rows)

	print(f"\n== {path.name} ==")
	print(f"  rows: {len(rows)}   changed: {changed_count}   needs_review: {review_count}")
	print(f"  mapping TSV: {map_out.name}")
	if unmapped:
		print(f"  UNMAPPED tags ({sum(unmapped.values())} rows):")
		for v, n in sorted(unmapped.items(), key=lambda x: -x[1]):
			print(f"    {n:4d}  {v!r}")


def main() -> None:
	process_file(
		HERE / "organizations_clustered.tsv",
		type_column="_ - organizations - _ - org_type",
		id_column="cluster_id",
	)
	process_file(
		HERE / "org_alignment_review.tsv",
		type_column="org_type",
		id_column="cluster_id",
	)
	process_file(
		HERE / "org_addresses_review.tsv",
		type_column="org_type",
		id_column="db_id",
		use_id_overrides=True,
	)


if __name__ == "__main__":
	main()
