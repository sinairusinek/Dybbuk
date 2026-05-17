"""LLM second-opinion sample using Sonnet 4.6.

Samples ~30 rows from each of the most-populous keyword-resolved buckets in the
mentions file, then asks Sonnet 4.6 to confirm or correct each row's canonical
type given (name, original_tag, sentence). Uses prompt caching for the system
prompt + canonical-type definitions, which are identical across all calls.
"""
from __future__ import annotations
import csv
import json
import os
import random
import sys
from collections import defaultdict
from pathlib import Path

csv.field_size_limit(sys.maxsize)
random.seed(20260512)

import anthropic

HERE = Path(__file__).parent
MENTIONS = HERE / "organizations_clustered.tsv"
MAPPING = HERE / "organizations_clustered_canonical_mapping.tsv"
OUT = HERE / "llm_review_sample.tsv"

MODEL = "claude-sonnet-4-5"  # Sonnet 4.6 alias on API
SAMPLE_PER_BUCKET = 30
BUCKETS = [
	"Theatre", "Traveling Company", "Theatre-related Society/ Union",
	"Labour (factory/workshop)", "Business",
	"Jewish political bodies", "Non-Jewish political bodies",
	"Welfare/Aid organization",
]

CANONICAL_TYPES = [
	"Theatre", "Traveling Company", "Company on Tour", "Amateur", "Kleinkunst",
	"Circus", "Theatre education",
	"Publisher", "Printer", "Printer/Publisher",
	"Journals/ Newspapers", "Media (Radio/ Film/TV)",
	"Library", "Heritage Institution", "Education",
	"Musical organization", "Theatre-related Society/ Union",
	"Religious institutions/organizations",
	"Jewish political bodies", "Non-Jewish political bodies",
	"Welfare/Aid organization",
	"Business", "Labour (factory/workshop)", "Health institutions",
	"Military", "Not an organization", "OTHER - elaborate!",
]

SYSTEM_PROMPT = """You are reviewing automated classification of Yiddish theatre-lexicon (Zalmen Zylbercweig) organization mentions into a fixed canonical typology of 27 types.

Use EXACTLY these strings as canonical types:

PERFORMANCE
- Theatre — established theatre institution with a venue.
- Traveling Company — touring is the core operational identity; no permanent home stage. Yiddish "X's troupe" / Vilner Trupe / Lodzer Trupe etc.
- Company on Tour — has home venue, currently on tour (temporary state). Use only when text makes this explicit.
- Amateur — amateur theatre groups, drama circles, theatre clubs.
- Kleinkunst — vaudeville, cabaret, variety theatre.
- Circus
- Theatre education — drama schools, theatre studios, acting studios for training performers.

PRINT / MEDIA
- Publisher (incl. lexicons, almanacs)
- Printer (drukerei; print shop)
- Printer/Publisher (combined)
- Journals/ Newspapers (serial publications)
- Media (Radio/ Film/TV) (radio, film, TV, news agencies, film companies)

KNOWLEDGE / MEMORY
- Library
- Heritage Institution — archives, museums, galleries (when as museum), Yiddish cultural-research institutes (YIVO, Kultur-Lige, IKUF, Sholem-Aleichem Institute, Leivick House).
- Education — schools, universities, non-theatre research institutes, Yiddish educational summer camps (Boiberik, Kinderland, Lakeland).

MUSIC
- Musical organization — orchestras, secular choirs, choral societies, music ensembles/associations (e.g. מוזיק-פאַראיין, געזאַנגס-פאַריין, Hazomir/הזמיר). SYNAGOGUE CHOIRS are RELIGIOUS, not musical.

THEATRE-INDUSTRY MEMBERSHIP
- Theatre-related Society/ Union — actors/artists/writers/musicians unions, theatre associations, dramatic societies, theatre committees. Tag "society"/"union" alone does NOT qualify — must have theatre/arts/writers/musicians cue.

RELIGION
- Religious institutions/organizations — synagogues, yeshivot, hasidic courts, religious choirs, churches, religious courts, kehilot (when religious-communal).

POLITICAL (two distinct buckets!)
- Jewish political bodies — Jewish national / Zionist / Bundist / Labor-Zionist / religious-political organizations and their funds/foundations. Includes:
    * Zionist parties: Poale Zion, Mizrachi, Hashomer Hatzair, Agudath Israel, Revisionists/Hatzohar.
    * Bund / Algemeyner Yidisher Arbeter Bund.
    * Israeli political: Knesset, Israeli parties, Histadrut.
    * Zionist funds: Keren Hayesod, JNF / Keren Kayemet / נאַציאָנאַל-פֿאָנד, Israel Bonds.
    * Judenrats (יודענראַט / יידנראָט) — ALWAYS flag as PI dilemma.
    * World Jewish Congress, Yiddish World Congress (when political-organizational).
    * Workmen's fraternal-political societies (Arbeter Ring / Workmen's Circle / אַרבעטער-רינג; Yidish-Natsionaler Arbeter-Farband). ALWAYS flag as PI dilemma — dual cultural-fraternal AND labour-political identity.
- Non-Jewish political bodies — non-Jewish governments, parliaments, ministries, councils, parties (Polish Sejm, Soviet commissariats, US Department of X, Communist Party of Poland, Polish Socialist Party). City councils, courts, embassies, executive committees that are not Jewish-communal.

WELFARE & MUTUAL AID
- Welfare/Aid organization — Jewish and general welfare, philanthropic, mutual aid, immigration aid, relief, communal welfare. Examples: HIAS, JDC/Joint, ORT, UNRRA, UJA, ADL, Hadassah, WIZO, mutual-aid burial societies (חסד של אמת), old age homes (מושב זקנים), social self-help (יידישער אַליינהילף, יידישער סאָציאַלער אַליינהילף), Jewish Welfare Board.
    * WIZO and Hadassah → Welfare/Aid (women's welfare + hospitals), NOT Jewish political, even though Zionist in name.
    * UJA / Federation → Welfare/Aid (fundraising for welfare), NOT Jewish political.

COMMERCE / LABOUR / HEALTH
- Business — commercial enterprises: banks, insurance, hotels, restaurants, cafés, saloons, shops, stores, firms, law firms, breweries (when OWNERSHIP relation), motor companies, telegraph companies, financial corporations, booking offices, galleries (when commercial).
- Labour (factory/workshop) — PLACES of physical labour: factories, sweatshops, workshops, breweries (when EMPLOYMENT relation), tailor shops, bakeries. EXCLUDES labour movements, workers' parties, fraternal-political organizations (those are political).
- Health institutions — hospitals, clinics, sanatoria, infirmaries. Red Cross / רויטן קרייץ here.

MILITARY
- Military — armies, military units, self-defense organizations (Jewish Legion / יידישן לעגיאָן; Jewish Self-defense / יידישן זעלבסטשוץ), partisans. Specific armies (Polish Army, Red Army, American Army) here.

SENTINELS
- Not an organization — places mis-tagged as orgs: ghettos, concentration camps (Majdanek, Janowska, Auschwitz, etc.), labor camps, refugee camps, parks (Seaside Park, Central Park), colonies, residences.
- OTHER - elaborate! — fallback for entities that genuinely fit no canonical type.

KEY DISAMBIGUATION RULES:
1. `אַרבעטער` (worker) is NOT automatically Labour. Workmen's Circle / Arbeter-Ring is Jewish political (with flag). Arbeter-Farband is Jewish political (with flag). אַרבעטער-קאָמיסאַריאַט is Non-Jewish political (Soviet government). Only literal factories/workshops are Labour.
2. Music-specific names (Hazomir, music association, choral society) → Musical organization, NOT Society/Union, even if "פאַראיין" is in the name.
3. Brewery/factory/workshop: depends on the person's relation. Ownership → Business; Employment → Labour. If relation is unclear, default Business and flag.
4. "X Company" without theatre/film cue: usually Business (insurance company, motor company, hotel firm). Only theatre/film context routes to Traveling Company or Media.
5. Synagogue choir → Religious (per canon), not Musical.
6. Educational summer camps (Boiberik, Kinderland) → Education, NOT Not-an-organization.

INPUT FORMAT (what I'll send you):
- Entity name (Yiddish/Hebrew/English)
- Original LLM tag
- Relation category (if available): Leadership_Ownership / Employment_Performance / Production_Distribution / Affiliation_Membership
- Sentence (excerpt of source text)
- Auto-assigned canonical type (the v3 mapper's choice)

OUTPUT FORMAT (strict JSON, ONE LINE only):
{"canonical": "<one of the 27 types EXACTLY>", "agree": true|false, "reason": "<<=25 words>"}

`agree` is true iff your canonical equals the auto-assigned one I send you."""


def load_sample() -> list[dict]:
	# load mapping (per-row decisions with metadata) and join with mention rows for sentence
	by_id: dict[str, dict] = {}
	with MAPPING.open(newline="", encoding="utf-8") as f:
		for r in csv.DictReader(f, delimiter="\t"):
			by_id.setdefault(r["row_id"], []).append(r)

	# read mention file once to extract sentence per row
	sent_col = "_ - organizations - _ - relations - _ - original_sentence"
	cid_col = "cluster_id"
	# Build (cluster_id → list of sentence_excerpt) but mapping uses row_id == cluster_id;
	# multiple mentions per cluster — pair mapping rows positionally with mention rows.
	# Simpler: iterate mention file in parallel, attaching sentence to mapping.
	by_bucket: dict[str, list[dict]] = defaultdict(list)
	with MENTIONS.open(newline="", encoding="utf-8") as fm, \
		 MAPPING.open(newline="", encoding="utf-8") as fp:
		mreader = csv.DictReader(fm, delimiter="\t")
		preader = csv.DictReader(fp, delimiter="\t")
		for mrow, prow in zip(mreader, preader):
			if prow.get("decided_via") != "context":
				continue
			canon = prow.get("canonical_type", "")
			if canon not in BUCKETS:
				continue
			sentence = mrow.get(sent_col, "")
			name = prow.get("name", "") or mrow.get("clustered organization", "")
			relation = mrow.get("_ - organizations - _ - relations - _ - category", "")
			by_bucket[canon].append({
				"row_id": prow.get("row_id", ""),
				"name": name,
				"original_type": prow.get("original_type", ""),
				"auto_canonical": canon,
				"sentence": sentence,
				"relation": relation,
			})

	picks: list[dict] = []
	for b in BUCKETS:
		rows = by_bucket.get(b, [])
		random.shuffle(rows)
		picks.extend(rows[:SAMPLE_PER_BUCKET])
	return picks


def main() -> None:
	if not os.environ.get("ANTHROPIC_API_KEY"):
		sys.exit("ANTHROPIC_API_KEY not set")
	picks = load_sample()
	print(f"Sample size: {len(picks)} rows across buckets {BUCKETS}")
	client = anthropic.Anthropic()

	results: list[dict] = []
	for i, row in enumerate(picks, 1):
		user_msg = (
			f"Entity name: {row['name']}\n"
			f"Original LLM tag: {row['original_type']}\n"
			f"Relation category: {row.get('relation', '')}\n"
			f"Sentence: {row['sentence'][:600]}\n"
			f"Auto-assigned canonical: {row['auto_canonical']}\n\n"
			f"Reply with JSON only."
		)
		resp = client.messages.create(
			model=MODEL,
			max_tokens=200,
			system=[{"type": "text", "text": SYSTEM_PROMPT,
					 "cache_control": {"type": "ephemeral"}}],
			messages=[{"role": "user", "content": user_msg}],
		)
		text = resp.content[0].text.strip()
		try:
			# strip code fences if any
			t = text
			if t.startswith("```"):
				t = t.strip("`")
				t = t.split("\n", 1)[1] if "\n" in t else t
				t = t.rsplit("```", 1)[0] if "```" in t else t
			data = json.loads(t)
		except Exception:
			data = {"canonical": "", "agree": None, "reason": f"parse_err: {text[:120]}"}

		llm_canonical = (data.get("canonical") or "").strip()
		auto_canonical = row["auto_canonical"]
		agree = (llm_canonical == auto_canonical)
		results.append({
			**row,
			"llm_canonical": llm_canonical,
			"llm_agree": "yes" if agree else "no",
			"llm_reason": data.get("reason", ""),
			"input_tokens": resp.usage.input_tokens,
			"cache_read": getattr(resp.usage, "cache_read_input_tokens", 0) or 0,
			"cache_create": getattr(resp.usage, "cache_creation_input_tokens", 0) or 0,
			"output_tokens": resp.usage.output_tokens,
		})
		if i % 10 == 0 or i == len(picks):
			print(f"  {i}/{len(picks)} done")

	# Write TSV
	cols = ["row_id", "name", "original_type", "sentence", "auto_canonical",
			"llm_canonical", "llm_agree", "llm_reason",
			"input_tokens", "cache_read", "cache_create", "output_tokens"]
	with OUT.open("w", newline="", encoding="utf-8") as f:
		w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
		w.writeheader()
		for r in results:
			w.writerow({k: r.get(k, "") for k in cols})

	# Summary
	from collections import Counter
	agree_c = Counter(r["llm_agree"] for r in results)
	by_bucket_agree = defaultdict(lambda: [0, 0])  # bucket → [agree, total]
	for r in results:
		by_bucket_agree[r["auto_canonical"]][1] += 1
		if r["llm_agree"] == "yes":
			by_bucket_agree[r["auto_canonical"]][0] += 1

	tot_in = sum(r["input_tokens"] for r in results)
	tot_out = sum(r["output_tokens"] for r in results)
	tot_cache_r = sum(r["cache_read"] for r in results)
	tot_cache_c = sum(r["cache_create"] for r in results)
	# Sonnet 4.5 pricing: $3 / MTok input, $15 / MTok output, $0.30 cache-read, $3.75 cache-write
	cost = (tot_in - tot_cache_r) * 3 / 1e6 + tot_cache_r * 0.30 / 1e6 + tot_cache_c * 3.75 / 1e6 + tot_out * 15 / 1e6

	print(f"\nResults: agree={agree_c.get('yes',0)}, disagree={agree_c.get('no',0)}")
	print("By bucket (agree/total):")
	for b, (a, t) in by_bucket_agree.items():
		print(f"  {b:25}  {a}/{t}")
	print(f"\nTokens: in={tot_in:,} (cache_read={tot_cache_r:,}, cache_create={tot_cache_c:,}) out={tot_out:,}")
	print(f"Estimated cost: ${cost:.3f}")
	print(f"Output: {OUT.name}")


if __name__ == "__main__":
	main()
