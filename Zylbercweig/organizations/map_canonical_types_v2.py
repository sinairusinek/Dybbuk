"""V2: Two-pass org_type → canonical mapper.

Pass A: unambiguous tags mapped by tag alone.
Pass B: ambiguous tags (union, company, society, club, association, organization,
        studio, institute, academy, institution, group, committee, office, agency,
        lodge, foundation, fund) resolved per-row using keyword matching against the
        entity name + (where available) the original_sentence.

Anything that cannot be resolved by Pass B keeps its original value and is flagged
needs_review=yes. Nothing gets a default guess.
"""
from __future__ import annotations
import csv
import re
import shutil
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

# ---------- Pass A: unambiguous tag → canonical ----------
TAG_MAP_A: dict[str, str] = {
	# Theatre
	"theatre": "Theatre", "Theatre": "Theatre",
	"טעאַטער": "Theatre", "בינע": "Theatre", "venue": "Theatre",
	# Traveling Company
	"troupe": "Traveling Company", "Troupe": "Traveling Company",
	"טרופּע": "Traveling Company", "theatre_group": "Traveling Company",
	"theatre troupe": "Traveling Company", "טעאַטער-טרופּע": "Traveling Company",
	"טעאַטער-קאָלעקטיוו": "Traveling Company",
	"Traveling Company": "Traveling Company",
	"Company on Tour": "Company on Tour",
	# Amateur
	"Amateur": "Amateur", "amateur troupe": "Amateur",
	"drama_circle": "Amateur", "drama club": "Amateur",
	"drama circle": "Amateur", "union drama circle": "Amateur",
	"theatre_club": "Amateur", "theatre club": "Amateur",
	# Theatre-related society
	"theatre_society": "Society/ Union",
	"theatre organization": "Society/ Union",
	"theatre society": "Society/ Union",
	"theatre_union": "Society/ Union",
	"theatre_collective": "Society/ Union",
	"ליטעראַריש-דראַמאַטישע געזעלשאַפט": "Society/ Union",
	# Theatre education
	"theatre studio": "Theatre education",
	# General education
	"school": "Education", "School": "Education", "Education": "Education",
	"university": "Education", "אוניווערזיטעט": "Education",
	"research institute": "Education", "student_organization": "Education",
	# Musical
	"choir": "Musical organization", "כאָר": "Musical organization",
	"orchestra": "Musical organization", "אָרקעסטער": "Musical organization",
	"band": "Musical organization", "philharmonic": "Musical organization",
	"Musical organization": "Musical organization",
	# Kleinkunst
	"Kleinkunst": "Kleinkunst",
	# Religious
	"synagogue": "Religious institutions/organizations",
	"shul": "Religious institutions/organizations",
	"שול": "Religious institutions/organizations",
	"temple": "Religious institutions/organizations",
	"house of prayer": "Religious institutions/organizations",
	"religious": "Religious institutions/organizations",
	"religious_institution": "Religious institutions/organizations",
	"religious_organization": "Religious institutions/organizations",
	"religious_group": "Religious institutions/organizations",
	"religious court": "Religious institutions/organizations",
	"synagogue choir": "Religious institutions/organizations",
	"קהלה": "Religious institutions/organizations",
	"Religious institutions/organizations": "Religious institutions/organizations",
	# Political
	"party": "Political bodies", "פארטיי": "Political bodies",
	"פּאַרטיי-אָרגאַניזאַציע": "Political bodies",
	"political_party": "Political bodies", "political organization": "Political bodies",
	"political movement": "Political bodies", "political_circle": "Political bodies",
	"movement": "Political bodies", "revolutionary organization": "Political bodies",
	"party branch": "Political bodies",
	"parliament": "Political bodies", "government": "Political bodies",
	"government body": "Political bodies", "government_office": "Political bodies",
	"government_agency": "Political bodies", "government_department": "Political bodies",
	"government_administration": "Political bodies", "government building": "Political bodies",
	"state_institution": "Political bodies", "state_government": "Political bodies",
	"city council": "Political bodies", "council": "Political bodies",
	"executive committee": "Political bodies",
	"קאָמיטעט": "Political bodies", "קאָמיסאַריאָט": "Political bodies",
	"commission": "Political bodies", "congress": "Political bodies",
	"conference": "Political bodies", "embassy": "Political bodies",
	"court": "Political bodies", "administration": "Political bodies",
	"division": "Political bodies", "communal office": "Political bodies",
	"Gmina": "Political bodies", "Political bodies": "Political bodies",
	# Military
	"army": "Military", "אַרמיי": "Military", "ארמיי": "Military",
	"אַרמעע": "Military", "military": "Military",
	"military organization": "Military", "Military": "Military",
	# Media
	"radio": "Media (Radio/ Film/TV)", "tv": "Media (Radio/ Film/TV)",
	"television": "Media (Radio/ Film/TV)",
	"radio_station": "Media (Radio/ Film/TV)",
	"radio program": "Media (Radio/ Film/TV)",
	"broadcaster": "Media (Radio/ Film/TV)",
	"media": "Media (Radio/ Film/TV)",
	"Media (Radio/ Film)": "Media (Radio/ Film/TV)",
	"Media (Radio/ Film/TV)": "Media (Radio/ Film/TV)",
	"film": "Media (Radio/ Film/TV)",
	"film_company": "Media (Radio/ Film/TV)",
	"film_studio": "Media (Radio/ Film/TV)",
	"cinema studio": "Media (Radio/ Film/TV)",
	"film publisher": "Media (Radio/ Film/TV)",
	"news agency": "Media (Radio/ Film/TV)",
	# Journals/Newspapers
	"newspaper": "Journals/ Newspapers", "journal": "Journals/ Newspapers",
	"newspaper/journal": "Journals/ Newspapers",
	"periodical": "Journals/ Newspapers", "publication": "Journals/ Newspapers",
	"זשורנאַל": "Journals/ Newspapers",
	"Journals/ Newspapers": "Journals/ Newspapers",
	# Publisher / Printer
	"publisher": "Publisher", "Publisher": "Publisher",
	"printer": "Printer", "Printer": "Printer",
	"Printer/Publisher": "Printer/Publisher",
	"דרוקעריי": "Printer",
	# Heritage
	"museum": "Heritage Institution", "מוזיי": "Heritage Institution",
	"archive": "Heritage Institution", "gallery": "Heritage Institution",
	"exhibition": "Heritage Institution",
	"historical_location": "Heritage Institution",
	"Heritage Institution": "Heritage Institution",
	# Library
	"library": "Library", "Library": "Library",
	# Health
	"hospital": "Health institutions", "clinic": "Health institutions",
	"sanatorium": "Health institutions",
	"medical institution": "Health institutions",
	"Health institutions": "Health institutions",
	# Labour (workshop / factory unambiguous)
	"factory": "Labour", "workshop": "Labour", "workplace": "Labour",
	"guild": "Labour", "Labour": "Labour",
	# Business — single-word unambiguous
	"firm": "Business", "shop": "Business", "bank": "Business",
	"hotel": "Business", "restaurant": "Business", "cafe": "Business",
	"saloon": "Business", "tavern": "Business", "insurance": "Business",
	"law_firm": "Business", "brewery": "Business", "farm": "Business",
	"booking_office": "Business", "pharmacy": "Business",
	"business": "Business", "Business": "Business",
	# Circus
	"circus": "Circus", "Circus": "Circus",
	# Not an organization
	"ghetto": "Not an organization", "camp": "Not an organization",
	"concentration camp": "Not an organization",
	"refugee_camp": "Not an organization",
	"colony": "Not an organization", "home": "Not an organization",
	"children's home": "Not an organization",
	"residence": "Not an organization", "resort": "Not an organization",
	"book": "Not an organization", "lexicon": "Not an organization",
	"Not an organization": "Not an organization",
	# Identity passthrough
	"Society/ Union": "Society/ Union",
	"OTHER - elaborate!": "OTHER - elaborate!",
	"Theatre education": "Theatre education",
	# Empty
	"": "",
}

# Ambiguous tags handled in Pass B (NOT in TAG_MAP_A).
AMBIGUOUS = {
	"union", "Union", "יוניע", "פאַריין", "labor union", "union/association", "union branch",
	"company",
	"society", "געזעלשאַפט", "social organization", "social_relief_org",
	"philanthropic society", "welfare organization", "relief society",
	"cultural society", "culture society", "cultural_organization",
	"cultural department", "cultural_center",
	"קולטור-געזעלשאַפט", "kultur-gezelshaft",
	"club", "association",
	"organization", "Organization", "ארגאניזאציע", "org", "other",
	"institution", "institute", "אינסטיטוט", "academy",
	"group", "literary group", "literary circle", "literary collection",
	"kruzhok", "קרוזשאָק",
	"committee", "agency", "department", "office",
	"lodge", "fraternal", "fund", "foundation",
	"studio", "סטוּדיע",
	"youth_organization", "יוגנט-אָרגאַניזאַצע",
	"charity", "community", "community_org", "community_center",
	"community organization", "communal_organization",
	"production",
	"orphanage", "labor camp",
	"miscellany", "etc", "center", "festival", "expedition",
}

# ---------- keyword sets ----------
def _kw(*ws: str) -> list[str]: return [w.lower() for w in ws]

THEATRE_KW = _kw(
	"theatre", "theater", "drama", "dramatic", "stage", "actors", "actor",
	"artist", "artists", "artistic", "players", "ensemble", "troupe",
	"cabaret", "kleinkunst", "vaudeville",
	"טעאַטער", "דראַמאַטיש", "אַקטיאָר", "אַקטיאָרן", "אַרטיסט", "אַרטיסטן",
	"טרופּע", "אַנסאַמבל", "בינע", "סצענע", "שויפּילער", "שוישפּילער",
	"טעאַטריקל",
)
MUSIC_KW = _kw(
	"music", "musical", "musician", "musicians", "orchestra", "choir",
	"chorus", "band", "singers", "singer", "conservatory", "philharmonic",
	"קאָנסערוואַטאָריע", "אָרקעסטער", "כאָר", "מוזיק", "זינגער",
)
WRITERS_KW = _kw(
	"writers", "writer", "literary", "literature", "journalists", "journalist",
	"poets", "poet", "authors", "author",
	"שרײַבער", "ליטעראַריש", "זשורנאַליסט", "פּאָעט",
)
FILM_KW = _kw("film", "pictures", "cinema", "kino", "movie", "tv", "television", "radio",
	"פֿילם", "קינאָ", "ראַדיאָ")
LABOUR_KW = _kw(
	"printers", "printer", "factory", "workers", "worker", "tailors",
	"garment", "sweatshop", "carpenters", "bakers", "needle trades",
	"arbeiter", "arbeter", "trade union", "workmen",
	"אַרבעטער", "דרוקער", "שניידער", "פאַבריק", "אַרבעטער-רינג", "באַקער",
)
RELIGIOUS_KW = _kw(
	"synagogue", "yeshiva", "yeshive", "hasidic", "religious", "rabbi", "rabbinical",
	"jewish religious", "temple", "church", "mosque", "monastery",
	"שול", "ישיבה", "רבי", "חסיד", "תלמוד תורה", "חדר", "בית-מדרש",
	"בית מדרש", "קלויז",
)
POLITICAL_KW = _kw(
	"zionist", "socialist", "communist", "bund", "bundist", "revolutionary",
	"poale", "poalei", "labor zionist", "anarchist", "party", "political",
	"government", "ministry", "municipal", "council", "parliament",
	"ציוניסט", "סאָציאַליסט", "קאָמוניסט", "בונד", "פּאַרטיי", "פּועלי",
)
EDUCATION_KW = _kw(
	"school", "university", "gymnasium", "college", "academic", "students",
	"pupils", "education", "educational",
	"שול", "אוניווערזיטעט", "גימנאַזיע", "תלמיד",
)
DRAMA_SCHOOL_KW = _kw(
	"drama school", "theatre school", "theater school", "acting school",
	"drama studio", "theatre studio", "theater studio", "acting studio",
	"דראַמאַטיש-סטודיע", "טעאַטער-סטודיע", "טעאַטער-שול",
)
BUSINESS_KW = _kw(
	"& co", "and co", "inc", "ltd", "corp", "corporation", "insurance",
	"bank", "hotel", "restaurant", "shoe", "textile", "manufacturing",
	"trading", "tobacco", "oil", "cigarette", "telegraph", "motor",
	"financial", "savings", "loan", "department store", "shop",
	"קאָמפּ", "קאָמפּאַני", "קאָרפּאָר", "קאָרפֿאַר", "פֿירמע", "פירמע", "אינשורענס",
	"באַנק", "האָטעל", "פעקטאָר", "פֿעקטאָר", "מאָטאָר", "ברויעריי",
)
HEALTH_KW = _kw(
	"hospital", "clinic", "sanatorium", "medical", "health", "infirmary",
	"שפּיטאָל", "קליניק", "סאַנאַטאָריום", "אַמבולאַטאָריע",
)
YOUTH_KW = _kw(
	"youth", "young", "yugnt", "yugend", "yugendlikhe",
	"יוגנט", "יוגענט", "יונגע",
)
CHILDREN_KW = _kw("children", "kinder", "kindergarten", "קינדער")
JOURNAL_KW = _kw(
	"newspaper", "journal", "magazine", "periodical", "press", "weekly",
	"daily", "monthly", "צייטונג", "זשורנאַל", "וואָכנשריפט", "טאָגבלאַט",
)


def _has(text: str, kws: list[str]) -> bool:
	t = text.lower()
	return any(k in t for k in kws)


# Pass B resolver. Returns (canonical, resolved_bool).
# If resolved_bool is False, caller leaves original value and flags review.
def resolve_ambiguous(tag: str, name: str, sentence: str) -> tuple[str, bool]:
	ctx = f"{name}  ||  {sentence}".lower()

	# Universal early-outs by strong context cues
	def in_theatre_only() -> bool:
		# theatre context but NOT also a labour/business/political context
		return _has(ctx, THEATRE_KW)

	if tag in {"union", "Union", "יוניע", "פאַריין", "labor union",
			   "union/association", "union branch"}:
		# Theatre-industry → Society/Union
		if _has(ctx, THEATRE_KW + WRITERS_KW + MUSIC_KW):
			return "Society/ Union", True
		if _has(ctx, LABOUR_KW):
			return "Labour", True
		# Default for unresolved union (no theatre nor labour cue): Society/Union,
		# flagged context_weak. In the Zalmen lexicon (theatre/arts focus) most
		# unresolved unions are theatre-adjacent; user confirmed v2 judgment.
		return "Society/ Union", False

	if tag == "company":
		if _has(ctx, THEATRE_KW):
			# Distinguish traveling company vs. company on tour: default Traveling
			# unless explicit "on tour" / "tour" phrasing — flag for review either way.
			return "Traveling Company", True
		if _has(ctx, FILM_KW):
			return "Media (Radio/ Film/TV)", True
		if _has(ctx, BUSINESS_KW):
			return "Business", True
		# Default for "company" with no theatre/film cues: Business (per user — most
		# are insurance/motor/firm/etc.) but mark needs_review.
		return "Business", False

	if tag in {"society", "געזעלשאַפט", "social organization",
			   "philanthropic society", "welfare organization", "relief society",
			   "social_relief_org", "cultural society", "culture society",
			   "cultural_organization", "cultural department", "cultural_center",
			   "קולטור-געזעלשאַפט", "kultur-gezelshaft", "charity",
			   "community", "community_org", "community_center",
			   "community organization", "communal_organization"}:
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, RELIGIOUS_KW):
			return "Religious institutions/organizations", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		return "OTHER - elaborate!", False

	if tag in {"club", "association"}:
		if _has(ctx, THEATRE_KW):
			# club + theatre → Amateur; association + theatre → Society/Union
			return ("Amateur" if tag == "club" else "Society/ Union"), True
		if _has(ctx, WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		if _has(ctx, RELIGIOUS_KW):
			return "Religious institutions/organizations", True
		if _has(ctx, LABOUR_KW):
			return "Labour", True
		return "OTHER - elaborate!", False

	if tag in {"organization", "Organization", "ארגאניזאציע", "org", "other"}:
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		if _has(ctx, RELIGIOUS_KW):
			return "Religious institutions/organizations", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		if _has(ctx, YOUTH_KW):
			return "Education", True
		if _has(ctx, LABOUR_KW):
			return "Labour", True
		return "OTHER - elaborate!", False

	if tag in {"institution"}:
		if _has(ctx, RELIGIOUS_KW):
			return "Religious institutions/organizations", True
		if _has(ctx, HEALTH_KW):
			return "Health institutions", True
		if _has(ctx, EDUCATION_KW + DRAMA_SCHOOL_KW):
			return ("Theatre education" if _has(ctx, THEATRE_KW) else "Education"), True
		if _has(ctx, THEATRE_KW):
			return "Theatre education", True
		return "OTHER - elaborate!", False

	if tag in {"institute", "אינסטיטוט"}:
		if _has(ctx, THEATRE_KW):
			return "Theatre education", True
		if _has(ctx, EDUCATION_KW) or _has(ctx, ["research"]):
			return "Education", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		return "Education", False  # weak default

	if tag == "academy":
		if _has(ctx, THEATRE_KW):
			return "Theatre education", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		return "Education", False

	if tag in {"studio", "סטוּדיע"}:
		if _has(ctx, FILM_KW):
			return "Media (Radio/ Film/TV)", True
		if _has(ctx, THEATRE_KW + DRAMA_SCHOOL_KW):
			return "Theatre education", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		return "", False

	if tag in {"group", "literary group", "literary circle", "literary collection",
			   "kruzhok", "קרוזשאָק"}:
		if _has(ctx, THEATRE_KW):
			return "Amateur", True
		if _has(ctx, WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, MUSIC_KW):
			return "Musical organization", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		return "OTHER - elaborate!", False

	if tag == "committee":
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		return "Political bodies", False

	if tag in {"agency"}:
		if _has(ctx, ["news", "press", "telegraph"]):
			return "Media (Radio/ Film/TV)", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		if _has(ctx, BUSINESS_KW):
			return "Business", True
		return "", False

	if tag in {"department", "office"}:
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		if _has(ctx, BUSINESS_KW):
			return "Business", True
		return "Political bodies", False

	if tag in {"lodge", "fraternal"}:
		return "OTHER - elaborate!", False

	if tag in {"fund", "foundation"}:
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return "Society/ Union", True
		if _has(ctx, RELIGIOUS_KW):
			return "Religious institutions/organizations", True
		return "OTHER - elaborate!", False

	if tag in {"youth_organization", "יוגנט-אָרגאַניזאַצע"}:
		if _has(ctx, THEATRE_KW):
			return "Amateur", True
		if _has(ctx, POLITICAL_KW):
			return "Political bodies", True
		return "Education", False

	if tag == "production":
		if _has(ctx, THEATRE_KW + FILM_KW):
			return "Media (Radio/ Film/TV)" if _has(ctx, FILM_KW) else "Theatre", True
		return "Business", False

	if tag == "orphanage":
		return "Not an organization", False

	if tag == "labor camp":
		return "Not an organization", True  # camps are places

	if tag in {"miscellany", "etc", "center", "festival", "expedition"}:
		return "OTHER - elaborate!", False

	return "", False


# Row-id overrides (db only).
ID_OVERRIDES: dict[str, tuple[str, bool]] = {
	"484": ("Education", True),
	"508": ("Labour", True),
	"558": ("Musical organization", False),
	"493": ("Society/ Union", True),
}


def process_file(
	path: Path,
	type_column: str,
	id_column: str | None = None,
	name_columns: list[str] | None = None,
	sentence_column: str | None = None,
	use_id_overrides: bool = False,
) -> None:
	backup = path.with_suffix(path.suffix + ".pre_canonical_backup")
	if not backup.exists():
		shutil.copy2(path, backup)

	with path.open(newline="", encoding="utf-8") as f:
		reader = csv.DictReader(f, delimiter="\t")
		fieldnames = reader.fieldnames or []
		rows = list(reader)

	name_col = None
	for c in name_columns or []:
		if c in fieldnames:
			name_col = c
			break

	mapping_rows: list[list[str]] = []
	unresolved_tags: dict[str, int] = {}
	review_count = 0
	changed_count = 0
	unmapped_tags: dict[str, int] = {}

	for row in rows:
		orig = row.get(type_column, "")
		row_id = row.get(id_column, "") if id_column else ""
		name = row.get(name_col, "") if name_col else ""
		sentence = row.get(sentence_column, "") if sentence_column else ""

		new = orig
		needs_review = False
		decided_via = ""

		if use_id_overrides and row_id in ID_OVERRIDES:
			new, needs_review = ID_OVERRIDES[row_id]
			decided_via = "id_override"
		elif orig in TAG_MAP_A:
			new = TAG_MAP_A[orig]
			decided_via = "tag"
		elif orig in AMBIGUOUS:
			resolved_val, ok = resolve_ambiguous(orig, name, sentence)
			if ok:
				new = resolved_val
				decided_via = "context"
			else:
				if resolved_val:
					new = resolved_val
					needs_review = True
					decided_via = "context_weak"
					unresolved_tags[orig] = unresolved_tags.get(orig, 0) + 1
				else:
					new = orig
					needs_review = True
					decided_via = "unresolved"
					unresolved_tags[orig] = unresolved_tags.get(orig, 0) + 1
		else:
			# Unknown tag — flag.
			new = orig
			needs_review = True
			decided_via = "unknown_tag"
			unmapped_tags[orig] = unmapped_tags.get(orig, 0) + 1

		row[type_column] = new
		if needs_review:
			review_count += 1
		if new != orig:
			changed_count += 1

		mapping_rows.append([
			row_id,
			name,
			orig,
			new,
			"yes" if new != orig else "",
			"yes" if needs_review else "",
			decided_via,
		])

	with path.open("w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
		writer.writeheader()
		writer.writerows(rows)

	map_out = path.with_name(path.stem + "_canonical_mapping.tsv")
	with map_out.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["row_id", "name", "original_type", "canonical_type",
					"changed", "needs_review", "decided_via"])
		w.writerows(mapping_rows)

	print(f"\n== {path.name} ==")
	print(f"  rows: {len(rows)}   changed: {changed_count}   needs_review: {review_count}")
	print(f"  mapping TSV: {map_out.name}")
	if unresolved_tags:
		print("  Pass-B unresolved/weak (flagged) by tag:")
		for v, n in sorted(unresolved_tags.items(), key=lambda x: -x[1]):
			print(f"    {n:5d}  {v!r}")
	if unmapped_tags:
		print("  UNKNOWN tags (not in TAG_MAP_A or AMBIGUOUS):")
		for v, n in sorted(unmapped_tags.items(), key=lambda x: -x[1]):
			print(f"    {n:5d}  {v!r}")


def main() -> None:
	process_file(
		HERE / "organizations_clustered.tsv",
		type_column="_ - organizations - _ - org_type",
		id_column="cluster_id",
		name_columns=["clustered organization", "_ - organizations - _ - title",
					   "_ - organizations - _ - descriptive_name"],
		sentence_column="_ - organizations - _ - relations - _ - original_sentence",
	)
	process_file(
		HERE / "org_alignment_review.tsv",
		type_column="org_type",
		id_column="cluster_id",
		name_columns=["canonical_yiddish", "name_variants"],
		sentence_column="name_variants",  # use variants in lieu of sentence
	)
	process_file(
		HERE / "org_addresses_review.tsv",
		type_column="org_type",
		id_column="db_id",
		name_columns=["canonical_yiddish"],
		sentence_column=None,
		use_id_overrides=True,
	)
	process_file(
		HERE / "core_db.tsv",
		type_column="org_type",
		id_column="db_id",
		name_columns=["name"],
		sentence_column=None,
		use_id_overrides=True,
	)


if __name__ == "__main__":
	main()
