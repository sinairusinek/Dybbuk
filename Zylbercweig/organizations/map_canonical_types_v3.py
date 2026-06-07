"""V3: Canonical org_type mapper.

Implements the policy in CLASSIFICATION_POLICY.md. 27 canonical types:
- Renamed: Society/ Union → Theatre-related Society/ Union
           Labour → Labour (factory/workshop)
- Split:  Political bodies → Jewish political bodies + Non-Jewish political bodies
- Added:  Welfare/Aid organization

Pass A: unambiguous tag → canonical.
Pass B: ambiguous tag → resolved per-row using
        (a) named-entity allow-lists (Arbeter Ring, HIAS, JDC, etc.),
        (b) keyword cascade against name + sentence + relation_category.

Outputs mapping TSVs with: row_id, name, original_type, canonical_type,
changed, needs_review, decided_via, review_reason.
"""
from __future__ import annotations
import csv
import re
import shutil
import sys
from pathlib import Path

csv.field_size_limit(sys.maxsize)
HERE = Path(__file__).parent

# ============================================================================
# CANONICAL TYPES (27)
# ============================================================================
CANON_THEATRE = "Theatre"
CANON_NON_YIDDISH_THEATRE = "Non-Yiddish Theatre"
CANON_TRAVELING = "Traveling Company"
CANON_ON_TOUR = "Company on Tour"
CANON_AMATEUR = "Amateur"
CANON_KLEINKUNST = "Kleinkunst"
CANON_CIRCUS = "Circus"
CANON_TH_EDU = "Theatre education"
CANON_PUBLISHER = "Publisher"
CANON_PRINTER = "Printer"
CANON_PRINT_PUB = "Printer/Publisher"
CANON_JOURNAL = "Journals/ Newspapers"
CANON_MEDIA = "Media (Radio/ Film/TV)"
CANON_LIBRARY = "Library"
CANON_HERITAGE = "Heritage Institution"
CANON_EDUCATION = "Education"
CANON_MUSICAL = "Musical organization"
CANON_TH_SOC = "Theatre-related Society/ Union"
CANON_RELIGIOUS = "Religious institutions/organizations"
CANON_JEW_POL = "Jewish political bodies"
CANON_NONJEW_POL = "Non-Jewish political bodies"
CANON_WELFARE = "Welfare/Aid organization"
CANON_BUSINESS = "Business"
CANON_LABOUR = "Labour (factory/workshop)"
CANON_HEALTH = "Health institutions"
CANON_MILITARY = "Military"
CANON_NOT_ORG = "Not an organization"
CANON_OTHER = "OTHER - elaborate!"
CANON_TRADE_UNION = "Trade Union / Professional Association"
CANON_JUDENRAT = "Judenrat"
CANON_SPORTS = "Sports/Recreation"
CANON_FRATERNAL = "Fraternal order"

ALL_CANON = {
	CANON_THEATRE, CANON_NON_YIDDISH_THEATRE,
	CANON_TRAVELING, CANON_ON_TOUR, CANON_AMATEUR, CANON_KLEINKUNST,
	CANON_CIRCUS, CANON_TH_EDU, CANON_PUBLISHER, CANON_PRINTER, CANON_PRINT_PUB,
	CANON_JOURNAL, CANON_MEDIA, CANON_LIBRARY, CANON_HERITAGE, CANON_EDUCATION,
	CANON_MUSICAL, CANON_TH_SOC, CANON_RELIGIOUS, CANON_JEW_POL, CANON_NONJEW_POL,
	CANON_WELFARE, CANON_BUSINESS, CANON_LABOUR, CANON_HEALTH, CANON_MILITARY,
	CANON_NOT_ORG, CANON_OTHER, CANON_TRADE_UNION,
	CANON_JUDENRAT, CANON_SPORTS, CANON_FRATERNAL,
}

# Legacy → canonical renames (for re-running over already-v2-mapped data)
LEGACY_RENAMES: dict[str, str] = {
	"Society/ Union": CANON_TH_SOC,
	"Labour": CANON_LABOUR,
	"Political bodies": CANON_NONJEW_POL,  # default rename; specific reclassification below
}

# ============================================================================
# Pass A — unambiguous tag → canonical
# ============================================================================
TAG_MAP_A: dict[str, str] = {
	# Theatre
	"theatre": CANON_THEATRE, "Theatre": CANON_THEATRE,
	"טעאַטער": CANON_THEATRE, "בינע": CANON_THEATRE, "venue": CANON_THEATRE,
	# Traveling Company
	"troupe": CANON_TRAVELING, "Troupe": CANON_TRAVELING,
	"טרופּע": CANON_TRAVELING, "theatre_group": CANON_TRAVELING,
	"theatre troupe": CANON_TRAVELING, "טעאַטער-טרופּע": CANON_TRAVELING,
	"טעאַטער-קאָלעקטיוו": CANON_TRAVELING,
	"Traveling Company": CANON_TRAVELING,
	"Company on Tour": CANON_ON_TOUR,
	# Amateur
	"Amateur": CANON_AMATEUR, "amateur troupe": CANON_AMATEUR,
	"drama_circle": CANON_AMATEUR, "drama club": CANON_AMATEUR,
	"drama circle": CANON_AMATEUR, "union drama circle": CANON_AMATEUR,
	"theatre_club": CANON_AMATEUR, "theatre club": CANON_AMATEUR,
	# Theatre-related society
	"theatre_society": CANON_TH_SOC,
	"theatre organization": CANON_TH_SOC,
	"theatre society": CANON_TH_SOC,
	"theatre_union": CANON_TH_SOC,
	"theatre_collective": CANON_TH_SOC,
	"ליטעראַריש-דראַמאַטישע געזעלשאַפט": CANON_TH_SOC,
	# Theatre education
	"theatre studio": CANON_TH_EDU,
	# General education
	"school": CANON_EDUCATION, "School": CANON_EDUCATION, "Education": CANON_EDUCATION,
	"university": CANON_EDUCATION, "אוניווערזיטעט": CANON_EDUCATION,
	"research institute": CANON_EDUCATION, "student_organization": CANON_EDUCATION,
	# Musical
	"choir": CANON_MUSICAL, "כאָר": CANON_MUSICAL,
	"orchestra": CANON_MUSICAL, "אָרקעסטער": CANON_MUSICAL,
	"band": CANON_MUSICAL, "philharmonic": CANON_MUSICAL,
	"Musical organization": CANON_MUSICAL,
	# Kleinkunst
	"Kleinkunst": CANON_KLEINKUNST, "vaudeville theater": CANON_KLEINKUNST,
	"cabaret": CANON_KLEINKUNST,
	# Religious
	"synagogue": CANON_RELIGIOUS, "shul": CANON_RELIGIOUS, "שול": CANON_RELIGIOUS,
	"temple": CANON_RELIGIOUS, "house of prayer": CANON_RELIGIOUS,
	"religious": CANON_RELIGIOUS,
	"religious_institution": CANON_RELIGIOUS,
	"religious_organization": CANON_RELIGIOUS,
	"religious_group": CANON_RELIGIOUS,
	"religious court": CANON_RELIGIOUS,
	"synagogue choir": CANON_RELIGIOUS,
	"קהלה": CANON_RELIGIOUS, "קהילה": CANON_RELIGIOUS,
	"church": CANON_RELIGIOUS,
	# Military
	"army": CANON_MILITARY, "אַרמיי": CANON_MILITARY, "ארמיי": CANON_MILITARY,
	"אַרמעע": CANON_MILITARY, "military": CANON_MILITARY,
	"military organization": CANON_MILITARY,
	# Media
	"radio": CANON_MEDIA, "tv": CANON_MEDIA, "television": CANON_MEDIA,
	"radio_station": CANON_MEDIA, "radio program": CANON_MEDIA,
	"radio stations": CANON_MEDIA, "broadcaster": CANON_MEDIA,
	"media": CANON_MEDIA, "Media (Radio/ Film)": CANON_MEDIA,
	"Media (Radio/ Film/TV)": CANON_MEDIA, "film": CANON_MEDIA,
	"film_company": CANON_MEDIA, "film_studio": CANON_MEDIA,
	"cinema studio": CANON_MEDIA, "cinema": CANON_MEDIA,
	"film publisher": CANON_MEDIA, "film_production": CANON_MEDIA,
	"news agency": CANON_MEDIA, "insurance_agency": CANON_BUSINESS,
	# Journals/Newspapers
	"newspaper": CANON_JOURNAL, "journal": CANON_JOURNAL,
	"newspaper/journal": CANON_JOURNAL, "periodical": CANON_JOURNAL,
	"publication": CANON_JOURNAL, "זשורנאַל": CANON_JOURNAL,
	"miscellany": CANON_JOURNAL,
	"Journals/ Newspapers": CANON_JOURNAL,
	# Publisher / Printer
	"publisher": CANON_PUBLISHER, "Publisher": CANON_PUBLISHER,
	"printer": CANON_PRINTER, "Printer": CANON_PRINTER,
	"Printer/Publisher": CANON_PRINT_PUB,
	"דרוקעריי": CANON_PRINTER,
	"lexicon": CANON_PUBLISHER,
	# Heritage
	"museum": CANON_HERITAGE, "מוזיי": CANON_HERITAGE,
	"archive": CANON_HERITAGE, "gallery": CANON_HERITAGE,
	"exhibition": CANON_HERITAGE,
	"historical_location": CANON_HERITAGE,
	"Heritage Institution": CANON_HERITAGE,
	# Library
	"library": CANON_LIBRARY, "Library": CANON_LIBRARY,
	# Health
	"hospital": CANON_HEALTH, "clinic": CANON_HEALTH,
	"sanatorium": CANON_HEALTH, "medical institution": CANON_HEALTH,
	"Health institutions": CANON_HEALTH,
	# Labour — only intrinsically-labour places
	"factory": CANON_LABOUR, "workshop": CANON_LABOUR, "workplace": CANON_LABOUR,
	"guild": CANON_LABOUR, "Labour": CANON_LABOUR,
	"Labour (factory/workshop)": CANON_LABOUR,
	# Business — single-word unambiguous
	"firm": CANON_BUSINESS, "shop": CANON_BUSINESS, "bank": CANON_BUSINESS,
	"hotel": CANON_BUSINESS, "restaurant": CANON_BUSINESS, "cafe": CANON_BUSINESS,
	"saloon": CANON_BUSINESS, "tavern": CANON_BUSINESS, "insurance": CANON_BUSINESS,
	"law_firm": CANON_BUSINESS, "store": CANON_BUSINESS,
	"booking_office": CANON_BUSINESS, "pharmacy": CANON_BUSINESS,
	"business": CANON_BUSINESS, "Business": CANON_BUSINESS,
	"insurance_agency": CANON_BUSINESS,
	# Circus
	"circus": CANON_CIRCUS, "Circus": CANON_CIRCUS,
	# Not an organization
	"ghetto": CANON_NOT_ORG, "concentration camp": CANON_NOT_ORG,
	"refugee_camp": CANON_NOT_ORG, "colony": CANON_NOT_ORG,
	"home": CANON_NOT_ORG, "children's home": CANON_NOT_ORG,
	"residence": CANON_NOT_ORG, "resort": CANON_NOT_ORG,
	"book": CANON_NOT_ORG,
	"Not an organization": CANON_NOT_ORG,
	# Identity passthroughs / renames-from-legacy
	"Society/ Union": CANON_TH_SOC,
	"Theatre-related Society/ Union": CANON_TH_SOC,
	"OTHER - elaborate!": CANON_OTHER,
	"Theatre education": CANON_TH_EDU,
	"Political bodies": CANON_NONJEW_POL,  # legacy rename; specific orgs reclassified by name allowlist
	"Jewish political bodies": CANON_JEW_POL,
	"Non-Jewish political bodies": CANON_NONJEW_POL,
	"Welfare/Aid organization": CANON_WELFARE,
	"Trade Union / Professional Association": CANON_TRADE_UNION,
	"Judenrat": CANON_JUDENRAT,
	"Sports/Recreation": CANON_SPORTS,
	"Fraternal order": CANON_FRATERNAL,
	# Empty
	"": "",
}

# Ambiguous tags handled in Pass B
AMBIGUOUS: set[str] = {
	# Unions
	"union", "Union", "יוניע", "פאַריין", "labor union", "union/association",
	"union branch",
	# Companies
	"company",
	# Societies / clubs / associations
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
	"camp",  # camps are ambiguous: educational vs. concentration
	"center", "festival", "expedition",
	# Political (require Jewish vs non-Jewish split)
	"party", "פארטיי", "פּאַרטיי-אָרגאַניזאַציע",
	"political_party", "political organization", "political movement",
	"political_circle", "political group", "movement", "revolutionary organization",
	"party branch", "parliament", "government", "government body",
	"government_office", "government_agency", "government_department",
	"government_administration", "government building", "state_institution",
	"state_government", "city council", "council", "executive committee",
	"קאָמיטעט", "קאָמיסאַריאָט", "commission", "congress", "conference",
	"embassy", "court", "administration", "division", "communal office",
	"Gmina", "Political bodies",
	# Brewery class (business vs labour by relation)
	"brewery", "bakery", "tailor shop", "farm",
}

# ============================================================================
# Named-entity allow-lists (highest precedence)
# ============================================================================
# Substring match on name (lowercased); first match wins.
NAMED_ENTITY_RULES: list[tuple[list[str], str, str]] = [
	# (name_substrings, canonical, review_reason)
	# Welfare/Aid
	(["hias", "האַיאַס", "היאָס"], CANON_WELFARE, ""),
	(["joint distribution", "דזשאינט"], CANON_WELFARE, ""),
	(["ort", "אָרט"], CANON_WELFARE, ""),
	(["unrra", "אונראַ"], CANON_WELFARE, ""),
	(["united jewish appeal", "uja", "יונייטער דזשואיש אַפּיל",
	  "פאַראייניקטן יידישן אַפּיל"], CANON_WELFARE, ""),
	(["jewish welfare board", "דזשואיש וועלפעיר"], CANON_WELFARE, ""),
	(["hadassah", "הדסה"], CANON_WELFARE,
	 "pi_dilemma:zionist_welfare_dual_identity"),
	(["wizo", "וויצאָ"], CANON_WELFARE,
	 "pi_dilemma:zionist_welfare_dual_identity"),
	(["anti-defamation", "anti defamation", "אַנטי דעפעמעישאָן"], CANON_WELFARE, ""),
	(["אַליינהילף", "selbsthilfe", "self-help", "self help"], CANON_WELFARE, ""),
	(["חסד של אמת"], CANON_WELFARE, ""),
	(["מושב זקנים", "old age home", "אַלטערס-היים"], CANON_WELFARE, ""),
	# Jewish political — Zionist funds & national bodies
	(["keren hayesod", "קרן היסוד"], CANON_JEW_POL, ""),
	(["keren kayemet", "jewish national fund", "national fund",
	  "נאַציאָנאַל-פֿאָנד", "נאַציאָנאַלפֿאַנד", "נאַציאָנאַל פאַנד"], CANON_JEW_POL, ""),
	(["israel bonds", "ישראל באָנדס"], CANON_JEW_POL, ""),
	(["histadrut", "הסתדרות"], CANON_JEW_POL, ""),
	(["world jewish congress", "yiddish world congress",
	  "וועלט-קאָנגרעס"], CANON_JEW_POL, ""),
	# Jewish political — fraternal-political with PI dilemma
	(["arbeter ring", "workmen's circle", "workmens circle",
	  "אַרבעטער-רינג", "אַרבעטער רינג"], CANON_JEW_POL,
	 "pi_dilemma:fraternal_political_dual_identity"),
	(["arbeter-farband", "arbeter farband",
	  "אַרבעטער-פֿאַרבאַנד", "אַרבעטער פֿאַרבאַנד"], CANON_JEW_POL,
	 "pi_dilemma:fraternal_political_dual_identity"),
	# Judenrats — own canonical type (PI decision 2026-05-12)
	(["judenrat", "יודענראַט", "יידנראָט", "judenrate"], CANON_JUDENRAT, ""),
	# Jewish political — Zionist parties & movements
	(["poale zion", "poalei zion", "פּועלי-ציון", "פּועלי ציון"], CANON_JEW_POL, ""),
	(["mizrachi", "מזרחי"], CANON_JEW_POL, ""),
	(["hashomer hatzair", "השומר הצעיר"], CANON_JEW_POL, ""),
	(["agudath israel", "agudat israel", "אַגודת ישראל"], CANON_JEW_POL, ""),
	(["revisionist", "hatzohar", "הצוהר"], CANON_JEW_POL, ""),
	# Jewish socialist political
	(["bund", "yidisher arbeter bund", "בונד", "אַלגעמיינער יידישער אַרבעטער-בונד",
	  "אַרבעטער-בונד"], CANON_JEW_POL, ""),
	# Heritage — Yiddish cultural-research institutes
	(["yivo", "ייוו\"אָ", "ייווא"], CANON_HERITAGE, ""),
	(["kultur-lige", "kultur lige", "קולטור-ליגע", "קולטור ליגע"], CANON_HERITAGE, ""),
	(["sholem-aleichem institute", "sholem aleichem institute",
	  "שלום-עליכם-אינסטיטוט"], CANON_HERITAGE, ""),
	(["ikuf", "איקוף"], CANON_HERITAGE, ""),
	(["leivick house", "לייוויק-הויז"], CANON_HERITAGE, ""),
	# Health
	(["red cross", "rotn kreyts", "רויטן קרייץ"], CANON_HEALTH, ""),
	# Musical
	(["hazomir", "hazamir", "הזמיר"], CANON_MUSICAL, ""),
	# Military — specific armies
	(["polish army", "פּוילישער אַרמיי", "פוילישער אַרמיי", "פּוילישע אַרמיי",
	  "פוילישע אַרמיי"], CANON_MILITARY, ""),
	(["red army", "rote armee", "rotn armee", "רויטער אַרמיי", "רויטער אַרמעע",
	  "rotn armiy"], CANON_MILITARY, ""),
	(["american army", "us army", "אַמעריקאַנער אַרמעע",
	  "אַמעריקאַנער אַרמיי"], CANON_MILITARY, ""),
	(["jewish legion", "יידישן לעגיאָן", "yidishn legyon"], CANON_MILITARY, ""),
	(["jewish self-defense", "self-defense", "selbstshuts",
	  "יידישן זעלבסטשוץ", "זעלבסטשוץ"], CANON_MILITARY, ""),
	(["partisans", "פּאַרטיזאַנער", "partizaner"], CANON_MILITARY, ""),
	# Education — Yiddish summer/educational camps
	(["camp boiberik", "קעמפּ בויבעריק", "boyberik"], CANON_EDUCATION, ""),
	(["camp kinderland", "קעמפּ קינדערלאַנד", "kinderland"], CANON_EDUCATION, ""),
	(["camp lakeland", "קעמפּ לעיקלאַנד", "lakeland"], CANON_EDUCATION, ""),
	# Sports/Recreation — Maccabi (PI decision 2026-05-12: own canonical)
	(["maccabi", "מכבי"], CANON_SPORTS, ""),
	# Grand Street Boys — informal street-based youth association (PI note)
	(["grand street boys", "גרענד סטריט-באָיס"], CANON_EDUCATION, ""),
	# Not an organization — concentration camps
	(["majdanek", "מאידאַנעק", "majdanek lager"], CANON_NOT_ORG, ""),
	(["janowska", "yanover lager", "יאַנאָווער לאַגער"], CANON_NOT_ORG, ""),
	(["auschwitz", "אוישוויץ"], CANON_NOT_ORG, ""),
	(["treblinka", "טרעבלינקע"], CANON_NOT_ORG, ""),
	# Not an organization — parks
	(["sea-side park", "seaside park", "סי-סייד פֿאַרק"], CANON_NOT_ORG, ""),
	(["central park"], CANON_NOT_ORG, ""),
]


# ============================================================================
# Keyword sets for Pass B keyword cascade
# ============================================================================
def _kw(*ws: str) -> list[str]: return [w.lower() for w in ws]

THEATRE_KW = _kw(
	"theatre", "theater", "drama", "dramatic", "stage", "actors", "actor",
	"artist", "artists", "artistic", "players", "ensemble", "troupe",
	"cabaret", "kleinkunst", "vaudeville", "theatrical",
	"טעאַטער", "דראַמאַטיש", "אַקטיאָר", "אַקטיאָרן", "אַרטיסט", "אַרטיסטן",
	"טרופּע", "אַנסאַמבל", "בינע", "סצענע", "שויפּילער", "שוישפּילער",
	"טעאַטריקל",
)
MUSIC_KW = _kw(
	"music", "musical", "musician", "musicians", "orchestra", "choir",
	"chorus", "band", "singers", "singer", "conservatory", "philharmonic",
	"song", "choral", "gezangs", "gezang",
	"קאָנסערוואַטאָריע", "אָרקעסטער", "כאָר", "מוזיק", "זינגער",
	"געזאַנגס", "געזאַנג",
)
WRITERS_KW = _kw(
	"writers", "writer", "literary", "literature", "journalists", "journalist",
	"poets", "poet", "authors", "author",
	"שרײַבער", "ליטעראַריש", "זשורנאַליסט", "פּאָעט",
)
FILM_KW = _kw("film", "pictures", "cinema", "kino", "movie", "tv", "television", "radio",
	"פֿילם", "קינאָ", "ראַדיאָ", "טעלעוויזיע")
LABOUR_PLACE_KW = _kw(
	# Strictly places of physical labour; NO "arbeter" generic (per Yiddish ambiguity)
	"factory", "sweatshop", "workshop", "workplace", "shop floor",
	"שאַפּ",  # shop (workplace meaning)
	"פאַבריק", "סוועטשאַפּ", "וואַרשטאַט",
)
RELIGIOUS_KW = _kw(
	"synagogue", "yeshiva", "yeshive", "hasidic", "religious", "rabbi", "rabbinical",
	"jewish religious", "temple", "church", "mosque", "monastery",
	"שול", "ישיבה", "רבי", "חסיד", "תלמוד תורה", "חדר", "בית-מדרש",
	"בית מדרש", "קלויז",
)
JEWISH_POLITICAL_KW = _kw(
	# Zionist & Jewish-national markers
	"zionist", "zionism", "ציוניסט", "ציוניזם",
	"poale", "poalei", "פּועלי",
	"bund", "bundist", "בונד", "בונדיסט",
	"jewish national", "national jewish",
	"hashomer hatzair", "השומר הצעיר",
	"mizrachi", "מזרחי",
	"agudath", "אַגודת",
	"revisionist", "hatzohar", "הצוהר",
	"keren", "קרן",
	"jewish congress", "world jewish",
	"yidisher arbeter bund",
	"jewish self-defense", "selbstshuts", "זעלבסטשוץ",
	# Judenrat
	"judenrat", "יודענראַט", "יידנראָט",
	# Israeli political
	"knesset", "כּנסת",
)
NONJEW_POLITICAL_KW = _kw(
	"government", "ministry", "parliament", "municipal", "council",
	"sejm", "duma", "congress", "senate", "department of",
	"city council", "court",
	"קאָמיסאַריאָט", "קאָמיטעט", "סטאַט", "רעגירונג", "פּאַרלאַמענט",
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
WELFARE_KW = _kw(
	"aid", "relief", "welfare", "philanthropic", "philanthropy",
	"charity", "charitable", "mutual aid", "immigrant aid",
	"settlement house", "fraternal society", "burial society",
	"orphans", "orphanage",
	"הילף", "אַליינהילף", "סאָציאַלע הילף", "וועלפעיר",
	"פֿאַראייניקטן יידישן", "יידישער", "appeal",
)
HEALTH_KW = _kw(
	"hospital", "clinic", "sanatorium", "medical", "health", "infirmary",
	"שפּיטאָל", "קליניק", "סאַנאַטאָריום", "אַמבולאַטאָריע",
)
YOUTH_KW = _kw(
	"youth", "young", "yugnt", "yugend", "yugendlikhe",
	"יוגנט", "יוגענט", "יונגע",
)


def _has(text: str, kws: list[str]) -> bool:
	t = text.lower()
	return any(k in t for k in kws)


# ============================================================================
# Pass B — resolve ambiguous tag with name + sentence + relation
# ============================================================================
# Boundary chars: latin word chars + Hebrew letters U+0590-05FF (incl. cantillation/vowels).
# A word-boundary "match" requires the substring to be bordered by either start/end of
# the name or a NON-word-or-hebrew character (space, dash, quote, punctuation).
_WORD_OR_HEB = re.compile(r"[\w֐-׿]")


def _word_boundary_match(needle: str, hay: str) -> bool:
	"""True iff `needle` appears in `hay` with non-word-or-Hebrew boundaries on both sides."""
	n = needle.lower()
	h = hay.lower()
	start = 0
	while True:
		i = h.find(n, start)
		if i < 0:
			return False
		left_ok = (i == 0) or not _WORD_OR_HEB.match(h[i - 1])
		right = i + len(n)
		right_ok = (right == len(h)) or not _WORD_OR_HEB.match(h[right])
		if left_ok and right_ok:
			return True
		start = i + 1


def _named_entity_match(name: str) -> tuple[str, str] | None:
	"""Check name against allow-list with word-boundary matching. Returns
	(canonical, review_reason) or None."""
	if not name:
		return None
	for substrs, canon, reason in NAMED_ENTITY_RULES:
		for s in substrs:
			if _word_boundary_match(s, name):
				return canon, reason
	return None


# ---- Sub-entity affiliation detection (Failure 3 flagging) ----
# Heads that signal the row is a SUB-UNIT (school, choir, etc.) of a larger parent.
_SUBENTITY_HEADS = [
	"שול", "שולן",  # school
	"סטודיע", "סטוּדיע",  # studio
	"סעקציע",  # section
	"קרייז",  # circle
	"ביבליאָטעק", "ביבליאטעק",  # library
	"פֿאַרלאַג", "פאַרלאַג",  # publishing arm
	"סעמינאָר",  # seminary
	"קורס",  # course
	"געזאַנג-פֿאַריין", "געזאַנגס-פֿאַריין",  # choral society
	"כאָר",  # choir
	"היים פֿאַר", "היים פאַר",  # home for X
	"קלוב",  # club
	"דראַמאַטישע סעקציע", "דראַמאַטישן קרייז",
	"טעאַטער-סטודיע", "טעאַטערסטודיע", "טעאַטער-שול",
	"לערער-סעמינאָר",  # teacher seminary
]
# Connectors that mean "belonging to": ביי / ביים / פֿון / אין + (optional ה/דער)
_AFFIL_RE = re.compile(
	r"(ביים?(\s+דער)?|פֿ?ון(\s+דער)?|אין(\s+דער)?)\s",
)


def _is_sub_entity_of_named(name: str) -> bool:
	"""True iff the name (a) contains a sub-entity head (school, choir, studio,
	section, library, publishing arm, etc.) AND (b) matches a named entity in
	the allow-list. The combination means we'd be auto-inheriting the parent's
	classification onto a sub-unit. Patterns covered:
	  - "<sub-head> ביי <parent>"   (e.g. דראַמאַטישע סעקציע ביים השומר הצעיר)
	  - "<parent> <sub-head>"        (e.g. אַרבעטער-רינג רינג שול)
	  - "<parent>-<sub-head>"        (e.g. איקוף-פֿאַרלאַג)
	"""
	if not name:
		return False
	n_low = name.lower()
	has_head = any(h.lower() in n_low for h in _SUBENTITY_HEADS)
	if not has_head:
		return False
	return _named_entity_match(name) is not None


def _political_route(ctx: str) -> str:
	"""Pick Jewish vs Non-Jewish political bucket from context."""
	if _has(ctx, JEWISH_POLITICAL_KW):
		return CANON_JEW_POL
	return CANON_NONJEW_POL


def resolve_ambiguous(
	tag: str, name: str, sentence: str, relation: str = "",
) -> tuple[str, bool, str]:
	"""Return (canonical, resolved_bool, review_reason).
	resolved_bool=True means high-confidence; False means weak/default + flag."""
	# Named entity check always first
	ne = _named_entity_match(name)
	if ne:
		canon, reason = ne
		return canon, (reason == ""), reason

	ctx = f"{name}  ||  {sentence}".lower()

	# ---- Unions ----
	if tag in {"union", "Union", "יוניע", "פאַריין", "labor union",
			   "union/association", "union branch"}:
		if _has(ctx, THEATRE_KW + WRITERS_KW + MUSIC_KW):
			# Music-specific → Musical organization (more specific than Society/Union)
			if _has(ctx, MUSIC_KW) and not _has(ctx, THEATRE_KW + WRITERS_KW):
				return CANON_MUSICAL, True, ""
			return CANON_TH_SOC, True, ""
		if _has(ctx, LABOUR_PLACE_KW):
			return CANON_LABOUR, True, ""
		# Generic trade union with no theatre cue — PI dilemma
		return CANON_TH_SOC, False, "context_weak:union_default_society"

	# ---- Companies ----
	if tag == "company":
		if _has(ctx, FILM_KW):
			return CANON_MEDIA, True, ""
		if _has(ctx, THEATRE_KW):
			return CANON_TRAVELING, True, ""
		if _has(ctx, BUSINESS_KW):
			return CANON_BUSINESS, True, ""
		return CANON_BUSINESS, False, "context_weak:company_no_cue"

	# ---- Brewery / factory-ambiguous (business vs labour by relation) ----
	if tag in {"brewery", "bakery", "tailor shop", "farm"}:
		rel = relation.lower()
		if "leadership_ownership" in rel or "ownership" in rel:
			return CANON_BUSINESS, True, ""
		if "employment_performance" in rel or "employment" in rel:
			return CANON_LABOUR, True, ""
		if "production_distribution" in rel:
			return CANON_BUSINESS, True, ""
		return CANON_BUSINESS, False, "pi_dilemma:brewery_relation_conflict"

	# ---- Political ----
	if tag in {"party", "פארטיי", "פּאַרטיי-אָרגאַניזאַציע", "political_party",
			   "political organization", "political movement", "political_circle",
			   "political group", "movement", "revolutionary organization",
			   "party branch", "Political bodies"}:
		# Music-related "movement" handled elsewhere; here always political.
		return _political_route(ctx), True, ""

	if tag in {"parliament", "government", "government body", "government_office",
			   "government_agency", "government_department",
			   "government_administration", "government building",
			   "state_institution", "state_government", "city council", "council",
			   "executive committee", "קאָמיסאַריאָט", "commission",
			   "congress", "conference", "embassy", "court", "administration",
			   "division", "communal office", "Gmina"}:
		return _political_route(ctx), True, ""

	if tag in {"committee", "קאָמיטעט"}:
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		return _political_route(ctx), True, ""

	if tag in {"agency"}:
		if _has(ctx, ["news", "press", "telegraph"]):
			return CANON_MEDIA, True, ""
		if _has(ctx, BUSINESS_KW):
			return CANON_BUSINESS, True, ""
		return _political_route(ctx), False, ""

	if tag in {"department", "office"}:
		if _has(ctx, BUSINESS_KW):
			return CANON_BUSINESS, True, ""
		return _political_route(ctx), False, ""

	# ---- Societies / clubs / associations / organizations ----
	society_tags = {"society", "געזעלשאַפט", "social organization",
					"philanthropic society", "welfare organization", "relief society",
					"social_relief_org", "cultural society", "culture society",
					"cultural_organization", "cultural department", "cultural_center",
					"קולטור-געזעלשאַפט", "kultur-gezelshaft",
					"community", "community_org", "community_center",
					"community organization", "communal_organization", "charity"}
	if tag in society_tags:
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, RELIGIOUS_KW):
			return CANON_RELIGIOUS, True, ""
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, NONJEW_POLITICAL_KW):
			return CANON_NONJEW_POL, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		# PI default 2026-05-12: generic society/club/association with no cue →
		# Trade Union / Professional Association (was Theatre-related Society/Union).
		return CANON_TRADE_UNION, False, "context_weak:society_default_trade_union"

	if tag in {"club", "association"}:
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		if _has(ctx, THEATRE_KW):
			return (CANON_AMATEUR if tag == "club" else CANON_TH_SOC), True, ""
		if _has(ctx, WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, NONJEW_POLITICAL_KW):
			return CANON_NONJEW_POL, True, ""
		if _has(ctx, RELIGIOUS_KW):
			return CANON_RELIGIOUS, True, ""
		if _has(ctx, LABOUR_PLACE_KW):
			return CANON_LABOUR, True, ""
		# PI default 2026-05-12.
		return CANON_TRADE_UNION, False, "context_weak:club_assoc_default_trade_union"

	if tag in {"organization", "Organization", "ארגאניזאציע", "org", "other"}:
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, NONJEW_POLITICAL_KW):
			return CANON_NONJEW_POL, True, ""
		if _has(ctx, RELIGIOUS_KW):
			return CANON_RELIGIOUS, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		if _has(ctx, YOUTH_KW):
			return CANON_EDUCATION, True, ""
		if _has(ctx, LABOUR_PLACE_KW):
			return CANON_LABOUR, True, ""
		# PI default 2026-05-12.
		return CANON_TRADE_UNION, False, "context_weak:organization_default_trade_union"

	# ---- Institutions / institutes / academies ----
	if tag in {"institution"}:
		if _has(ctx, RELIGIOUS_KW):
			return CANON_RELIGIOUS, True, ""
		if _has(ctx, HEALTH_KW):
			return CANON_HEALTH, True, ""
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		if _has(ctx, EDUCATION_KW + DRAMA_SCHOOL_KW):
			return (CANON_TH_EDU if _has(ctx, THEATRE_KW) else CANON_EDUCATION), True, ""
		if _has(ctx, THEATRE_KW):
			return CANON_TH_EDU, True, ""
		return CANON_OTHER, False, "unresolved"

	if tag in {"institute", "אינסטיטוט"}:
		if _has(ctx, THEATRE_KW):
			return CANON_TH_EDU, True, ""
		if _has(ctx, EDUCATION_KW) or _has(ctx, ["research"]):
			return CANON_EDUCATION, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		return CANON_EDUCATION, False, "context_weak:institute_default_education"

	if tag == "academy":
		if _has(ctx, THEATRE_KW):
			return CANON_TH_EDU, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		return CANON_EDUCATION, False, "context_weak:academy_default_education"

	# ---- Studios ----
	if tag in {"studio", "סטוּדיע"}:
		if _has(ctx, FILM_KW):
			return CANON_MEDIA, True, ""
		if _has(ctx, THEATRE_KW + DRAMA_SCHOOL_KW):
			return CANON_TH_EDU, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		return CANON_OTHER, False, "unresolved"

	# ---- Groups / literary circles ----
	if tag in {"group", "literary group", "literary circle", "literary collection",
			   "kruzhok", "קרוזשאָק"}:
		if _has(ctx, THEATRE_KW):
			return CANON_AMATEUR, True, ""
		if _has(ctx, WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, MUSIC_KW):
			return CANON_MUSICAL, True, ""
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, NONJEW_POLITICAL_KW):
			return CANON_NONJEW_POL, True, ""
		return CANON_OTHER, False, "unresolved"

	# ---- Lodge / fraternal / fund / foundation ----
	if tag in {"lodge", "fraternal"}:
		# Fraternal lodges/orders → own canonical (PI decision 2026-05-12).
		return CANON_FRATERNAL, True, ""

	if tag in {"fund", "foundation"}:
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		if _has(ctx, THEATRE_KW + WRITERS_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, RELIGIOUS_KW):
			return CANON_RELIGIOUS, True, ""
		return CANON_OTHER, False, "unresolved"

	# ---- Youth ----
	if tag in {"youth_organization", "יוגנט-אָרגאַניזאַצע"}:
		if _has(ctx, THEATRE_KW):
			return CANON_AMATEUR, True, ""
		if _has(ctx, JEWISH_POLITICAL_KW):
			return CANON_JEW_POL, True, ""
		if _has(ctx, NONJEW_POLITICAL_KW):
			return CANON_NONJEW_POL, True, ""
		return CANON_EDUCATION, False, "context_weak:youth_default_education"

	# ---- Production ----
	if tag == "production":
		if _has(ctx, FILM_KW):
			return CANON_MEDIA, True, ""
		if _has(ctx, THEATRE_KW):
			return CANON_THEATRE, True, ""
		return CANON_BUSINESS, False, "context_weak:production_default_business"

	# ---- Orphanage / Camps ----
	if tag == "orphanage":
		return CANON_WELFARE, True, ""

	if tag in {"camp", "labor camp"}:
		# Educational camp vs concentration camp routed via named-entity allow-list.
		# Default unresolved → flag.
		return CANON_NOT_ORG, False, "context_weak:camp_default_not_org"

	# ---- Center / festival / expedition ----
	if tag in {"center"}:
		if _has(ctx, THEATRE_KW):
			return CANON_TH_SOC, True, ""
		if _has(ctx, WELFARE_KW):
			return CANON_WELFARE, True, ""
		return CANON_OTHER, False, "unresolved"

	if tag in {"festival", "expedition"}:
		return CANON_OTHER, False, "unresolved"

	return "", False, "unresolved"


# ============================================================================
# Row-id overrides (db only)
# ============================================================================
ID_OVERRIDES: dict[str, tuple[str, str]] = {
	# (canonical, review_reason)
	"484": (CANON_EDUCATION, "pi_dilemma:ostrovski_institute_theatre_or_general"),
	"508": (CANON_TH_SOC, "pi_dilemma:vilna_printers_union_trade_or_theatre"),
	"558": (CANON_MUSICAL, ""),
	"493": (CANON_TH_SOC, "pi_dilemma:sao_paulo_yiddish_society_scope"),
}


# ============================================================================
# Processing
# ============================================================================
def process_file(
	path: Path,
	type_column: str,
	id_column: str | None = None,
	name_columns: list[str] | None = None,
	sentence_column: str | None = None,
	relation_column: str | None = None,
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
		relation = row.get(relation_column, "") if relation_column else ""

		new = orig
		needs_review = False
		decided_via = ""
		review_reason = ""

		if use_id_overrides and row_id in ID_OVERRIDES:
			new, review_reason = ID_OVERRIDES[row_id]
			needs_review = bool(review_reason)
			decided_via = "id_override"
		else:
			# Apply named-entity rule even if tag is in Pass A — named entities trump tag.
			ne = _named_entity_match(name) if name else None
			if ne and orig != "":
				canon, reason = ne
				new = canon
				# Sub-entity affiliation detection (Failure 3): if the name shows a
				# sub-unit (school, choir, studio, section, library, etc.) belonging
				# to a known named entity, the parent's classification is contestable.
				if _is_sub_entity_of_named(name):
					review_reason = "pi_dilemma:sub_entity_vs_parent_classification"
					needs_review = True
				else:
					review_reason = reason
					needs_review = bool(reason)
				decided_via = "named_entity"
			elif orig in TAG_MAP_A:
				new = TAG_MAP_A[orig]
				decided_via = "tag"
				# Apply legacy renames if the file was previously v2-mapped
				if orig in LEGACY_RENAMES:
					new = LEGACY_RENAMES[orig]
					decided_via = "legacy_rename"
			elif orig in LEGACY_RENAMES:
				new = LEGACY_RENAMES[orig]
				decided_via = "legacy_rename"
			elif orig in AMBIGUOUS:
				resolved_val, ok, reason = resolve_ambiguous(orig, name, sentence, relation)
				if ok:
					new = resolved_val
					decided_via = "context"
					review_reason = reason
				else:
					if resolved_val:
						new = resolved_val
						needs_review = True
						decided_via = "context_weak"
						review_reason = reason
						unresolved_tags[orig] = unresolved_tags.get(orig, 0) + 1
					else:
						new = orig
						needs_review = True
						decided_via = "unresolved"
						review_reason = reason or "unresolved"
						unresolved_tags[orig] = unresolved_tags.get(orig, 0) + 1
			else:
				new = orig
				needs_review = True
				decided_via = "unknown_tag"
				review_reason = "unknown_tag"
				unmapped_tags[orig] = unmapped_tags.get(orig, 0) + 1

		row[type_column] = new
		if needs_review:
			review_count += 1
		if new != orig:
			changed_count += 1

		mapping_rows.append([
			row_id, name, orig, new,
			"yes" if new != orig else "",
			"yes" if needs_review else "",
			decided_via, review_reason,
		])

	with path.open("w", newline="", encoding="utf-8") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
		writer.writeheader()
		writer.writerows(rows)

	map_out = path.with_name(path.stem + "_canonical_mapping.tsv")
	with map_out.open("w", newline="", encoding="utf-8") as f:
		w = csv.writer(f, delimiter="\t")
		w.writerow(["row_id", "name", "original_type", "canonical_type",
					"changed", "needs_review", "decided_via", "review_reason"])
		w.writerows(mapping_rows)

	print(f"\n== {path.name} ==")
	print(f"  rows: {len(rows)}   changed: {changed_count}   needs_review: {review_count}")
	print(f"  mapping TSV: {map_out.name}")
	if unresolved_tags:
		print("  Pass-B unresolved/weak (flagged) by tag (top 15):")
		for v, n in sorted(unresolved_tags.items(), key=lambda x: -x[1])[:15]:
			print(f"    {n:5d}  {v!r}")
	if unmapped_tags:
		print(f"  UNKNOWN tags ({sum(unmapped_tags.values())} rows):")
		for v, n in sorted(unmapped_tags.items(), key=lambda x: -x[1])[:10]:
			print(f"    {n:5d}  {v!r}")


def main() -> None:
	process_file(
		HERE / "organizations_clustered.tsv",
		type_column="_ - organizations - _ - org_type",
		id_column="cluster_id",
		name_columns=["clustered organization", "_ - organizations - _ - title",
					   "_ - organizations - _ - descriptive_name"],
		sentence_column="_ - organizations - _ - relations - _ - original_sentence",
		relation_column="_ - organizations - _ - relations - _ - category",
	)
	process_file(
		HERE / "org_alignment_review.tsv",
		type_column="org_type",
		id_column="cluster_id",
		name_columns=["canonical_yiddish", "name_variants"],
		sentence_column="name_variants",
		relation_column=None,
	)
	process_file(
		HERE / "org_addresses_review.tsv",
		type_column="org_type",
		id_column="db_id",
		name_columns=["canonical_yiddish"],
		sentence_column=None,
		relation_column=None,
		use_id_overrides=True,
	)
	process_file(
		HERE / "core_db.tsv",
		type_column="org_type",
		id_column="db_id",
		name_columns=["name"],
		sentence_column=None,
		relation_column=None,
		use_id_overrides=True,
	)


if __name__ == "__main__":
	main()
