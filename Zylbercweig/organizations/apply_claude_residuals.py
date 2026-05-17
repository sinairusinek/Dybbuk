"""Apply Claude Opus 4.7 (in-session) classifications to the 97 residual rows.

Each entry: (file_idx, row_idx, new_canonical_or_None, reason)
- new_canonical=None → confirm current value, unflag.
- new_canonical=str → override, unflag.
"""
import csv, sys
csv.field_size_limit(sys.maxsize)
from pathlib import Path
HERE = Path(__file__).parent

SPECS = [
    {"map": HERE / "organizations_clustered_canonical_mapping.tsv",
     "data": HERE / "organizations_clustered.tsv",
     "type_col": "_ - organizations - _ - org_type"},
    {"map": HERE / "org_alignment_review_canonical_mapping.tsv",
     "data": HERE / "org_alignment_review.tsv",
     "type_col": "org_type"},
]

# (file_idx, row_idx, new_canonical_or_None, reason)
DECISIONS = [
    # ============ MENTIONS (file_idx=0) ============
    # Statistical expedition organized by Jan Bloch — research expedition
    (0, 1259, "Education", "claude:statistical_research_expedition"),
    # Moliam — unclear acronym
    (0, 1293, None, "claude:unknown_acronym_unflagged"),
    # TSIB — unclear acronym
    (0, 1520, None, "claude:unknown_acronym_unflagged"),
    # Heintzel-Kunitzer — textile firm, employee = traveling salesman
    (0, 1776, None, "claude:confirmed_business"),
    # Government institution — clerk in Polish gov't agency
    (0, 2540, None, "claude:confirmed_nonjewish_political"),
    # Gdansk Magistrate
    (0, 2776, None, "claude:confirmed_nonjewish_political"),
    # Dembo labor camp
    (0, 2939, None, "claude:confirmed_camp_not_org"),
    # Kornfeld Brothers — wealthy household where person was secretary; not an org/business
    (0, 3824, "Not an organization", "claude:wealthy_household_not_org"),
    # Women's Division — chairwoman; likely women's welfare org
    (0, 4111, "Welfare/Aid organization", "claude:womens_welfare_div"),
    # American Savings and Loan Association
    (0, 4137, None, "claude:confirmed_business_financial"),
    # Citizens Family Insurance Society
    (0, 4138, None, "claude:confirmed_business_insurance"),
    # "Nit Gedaiget" Detroit group — Yiddish cultural/recreational group
    (0, 4166, None, "claude:detroit_yiddish_cultural_group_other"),
    # General Linen — firm founded by H. Shumer
    (0, 4197, None, "claude:confirmed_business_firm"),
    # Poniatów labor camp
    (0, 4407, None, "claude:confirmed_camp_not_org"),
    # I.K. Poznanski — Russian-German correspondent at the firm
    (0, 4471, None, "claude:confirmed_business_firm"),
    # Auschwitz
    (0, 4540, None, "claude:confirmed_camp_not_org"),
    # Poniatów camp
    (0, 4650, None, "claude:confirmed_camp_not_org"),
    # Daugmergen — German camp
    (0, 4800, None, "claude:confirmed_camp_not_org"),
    # Resha peat camp
    (0, 4801, None, "claude:confirmed_camp_not_org"),
    # P.P.V. (Plastic Plan Vilna) — Vilna artistic-research project (ghetto era)
    (0, 4898, "Heritage Institution", "claude:vilna_artistic_research_project"),
    # Black Keilis — Vilna labor camp
    (0, 5032, None, "claude:confirmed_labor_camp_not_org"),
    # Keilis — Vilna camp
    (0, 5187, None, "claude:confirmed_camp_not_org"),
    # Stutthof — concentration camp
    (0, 5188, None, "claude:confirmed_camp_not_org"),
    # Europa Insurance Society — silent partner, father-in-law director
    (0, 5700, None, "claude:confirmed_business_insurance"),
    # Tkuma — cultural-literary evenings, "main organizer of these evenings"
    (0, 6192, "Theatre-related Society/ Union", "claude:literary_cultural_society"),
    # Miami Beach Civic League
    (0, 6605, "Non-Jewish political bodies", "claude:miami_civic_league"),
    # "Club of the speizler" — workers' club for food sellers/grocers; trade union/professional
    (0, 8849, "Trade Union / Professional Association", "claude:food_workers_club"),
    # Singer Sewing Machines — manager
    (0, 10030, None, "claude:confirmed_business_manufacturer"),
    # "Yakor" Insurance Society
    (0, 12756, None, "claude:confirmed_business_insurance"),
    # Circle of Maskilim — Haskalah-era intellectual circle
    (0, 13192, "Heritage Institution", "claude:haskalah_intellectual_circle"),
    # Kharkov-Nikolaev Railway Line — newspaper seller on railway
    (0, 14265, None, "claude:confirmed_business_railway"),
    # Heintzel and Kunitzer Stock Company
    (0, 14482, None, "claude:confirmed_business_stock_company"),
    # Folks-Lige — folk/community welfare league
    (0, 15139, "Welfare/Aid organization", "claude:folk_welfare_league"),
    # Manishevits firm — kosher products manufacturer
    (0, 15193, None, "claude:confirmed_business_manufacturer"),
    # Zilberstein — large Jewish manufacturers
    (0, 15564, None, "claude:confirmed_business_manufacturers"),
    # Center in Dimona — community center
    (0, 16250, "Welfare/Aid organization", "claude:community_center_dimona"),
    # Friends of Progress — unclear org
    (0, 16267, None, "claude:unclear_friends_of_progress"),
    # Belgian Jewish Association — Jewish communal welfare association
    (0, 16402, "Welfare/Aid organization", "claude:belgian_jewish_welfare"),
    # Gezbir — unclear
    (0, 16409, None, "claude:unclear_acronym"),

    # ============ CLUSTERS (file_idx=1) ============
    # Peace Organization of Newcomers — immigrant welfare
    (1, 394, "Welfare/Aid organization", "claude:immigrant_welfare"),
    # Jewish Comrades Alliance — fraternal/welfare
    (1, 906, "Welfare/Aid organization", "claude:jewish_comrades_welfare"),
    # Statistical Expedition (cluster) — same as 1259
    (1, 935, "Education", "claude:statistical_research_expedition"),
    # Moliam (cluster)
    (1, 958, None, "claude:unknown_acronym_unflagged"),
    # Vladek Open Forums — public lecture/cultural forums; defensible as OTHER or Society
    (1, 1017, None, "claude:vladek_forums_other"),
    # TSIB (cluster)
    (1, 1081, None, "claude:unknown_acronym_unflagged"),
    # Heintzel-Kunitzer (cluster) — Business
    (1, 1234, None, "claude:confirmed_business"),
    # Mir Chicago — Jewish communal group
    (1, 1389, None, "claude:mir_chicago_unclear"),
    # Va'adat ha-tarbut — Culture Committee (Hebrew)
    (1, 1398, "Heritage Institution", "claude:culture_committee"),
    # Government institution (cluster)
    (1, 1652, None, "claude:confirmed_nonjewish_political"),
    # Polish Gardeners' Society — professional society
    (1, 1698, "Trade Union / Professional Association", "claude:polish_gardeners_society"),
    # Jewish Committee — ambiguous, keep Jewish political bodies
    (1, 1751, None, "claude:jewish_committee_kept"),
    # Gdansk Magistrate (cluster)
    (1, 1787, None, "claude:confirmed_nonjewish_political"),
    # Dembo camp (cluster)
    (1, 1880, None, "claude:confirmed_camp_not_org"),
    # Kornfeld Brothers (cluster) — household
    (1, 2373, "Not an organization", "claude:wealthy_household_not_org"),
    # New Jewish Americans
    (1, 2390, None, "claude:new_jewish_americans_unclear"),
    # Women's Division (cluster)
    (1, 2528, "Welfare/Aid organization", "claude:womens_welfare_div"),
    # American Savings and Loan (cluster)
    (1, 2548, None, "claude:confirmed_business_financial"),
    # Citizens Family Insurance (cluster)
    (1, 2549, None, "claude:confirmed_business_insurance"),
    # Nit Gedaiget (cluster)
    (1, 2567, None, "claude:detroit_yiddish_cultural_group_other"),
    # General Linen (cluster)
    (1, 2583, None, "claude:confirmed_business_firm"),
    # Poniatów (cluster)
    (1, 2706, None, "claude:confirmed_camp_not_org"),
    # I.K. Poznanski (cluster)
    (1, 2738, None, "claude:confirmed_business_firm"),
    # Auschwitz (cluster)
    (1, 2782, None, "claude:confirmed_camp_not_org"),
    # Poniatów camp (cluster)
    (1, 2842, None, "claude:confirmed_camp_not_org"),
    # Daugmergen (cluster)
    (1, 2916, None, "claude:confirmed_camp_not_org"),
    # Resha peat camp (cluster)
    (1, 2917, None, "claude:confirmed_camp_not_org"),
    # Kultur-Optailung — Culture Department
    (1, 2987, "Heritage Institution", "claude:culture_department"),
    # Black Keilis (cluster)
    (1, 3030, None, "claude:confirmed_labor_camp_not_org"),
    # Oneg Shabbos — Sabbath religious gathering (most common meaning)
    (1, 3091, "Religious institutions/organizations", "claude:oneg_shabbat_religious"),
    # Keilis (cluster)
    (1, 3112, None, "claude:confirmed_camp_not_org"),
    # Stutthof (cluster)
    (1, 3113, None, "claude:confirmed_camp_not_org"),
    # Europa Insurance (cluster)
    (1, 3350, None, "claude:confirmed_business_insurance"),
    # Tkuma (cluster)
    (1, 3572, "Theatre-related Society/ Union", "claude:literary_cultural_society"),
    # Miami Beach Civic League (cluster)
    (1, 3749, "Non-Jewish political bodies", "claude:miami_civic_league"),
    # Yepaka — acronym
    (1, 4707, None, "claude:unknown_acronym"),
    # Club "KIM" — possibly Soviet youth (Komsomol Internat. of Youth)
    (1, 4718, "Non-Jewish political bodies", "claude:club_kim_soviet_youth"),
    # Club of Transport Workers
    (1, 4719, "Trade Union / Professional Association", "claude:transport_workers_club"),
    # Folkbild — unclear ("folk picture")
    (1, 5236, None, "claude:folkbild_unclear"),
    # Baron Horace Ginzburg Expedition — research
    (1, 5496, "Education", "claude:ginzburg_research_expedition"),
    # Pennsylvania Railroad (cluster)
    (1, 5768, None, "claude:confirmed_business_railroad"),
    # Yakor (cluster)
    (1, 6233, None, "claude:confirmed_business_insurance"),
    # Circle of Maskilim (cluster)
    (1, 6385, "Heritage Institution", "claude:haskalah_intellectual_circle"),
    # Kharkov-Nikolaev railway (cluster)
    (1, 6748, None, "claude:confirmed_business_railway"),
    # Heintzel-Kunitzer stock company (cluster)
    (1, 6812, None, "claude:confirmed_business_stock_company"),
    # Poznanski firm (cluster)
    (1, 6894, None, "claude:confirmed_business_firm"),
    # Polish Alliance in Canada — ethnic mutual aid
    (1, 6952, "Welfare/Aid organization", "claude:polish_canadian_ethnic_alliance"),
    # Folks-Lige (cluster)
    (1, 7021, "Welfare/Aid organization", "claude:folk_welfare_league"),
    # Manishevits firm (cluster)
    (1, 7037, None, "claude:confirmed_business_manufacturer"),
    # Zilberstein (cluster)
    (1, 7147, None, "claude:confirmed_business_manufacturers"),
    # Sholem Aleichem Women's Organization — Jewish women's cultural-welfare
    (1, 7243, "Welfare/Aid organization", "claude:sholem_aleichem_womens_org"),
    # Center in Dimona (cluster)
    (1, 7398, "Welfare/Aid organization", "claude:community_center_dimona"),
    # Friends of Progress (cluster)
    (1, 7405, None, "claude:unclear_friends_of_progress"),
    # Hollywood Branch — branch of unknown org
    (1, 7407, None, "claude:hollywood_branch_unclear"),
    # Ezra — Jewish mutual aid/welfare society (Ezra=help)
    (1, 7468, "Welfare/Aid organization", "claude:ezra_jewish_welfare"),
    # Belgian Jewish Association (cluster)
    (1, 7472, "Welfare/Aid organization", "claude:belgian_jewish_welfare"),
    # Belgian Jewish Society
    (1, 7473, "Welfare/Aid organization", "claude:belgian_jewish_welfare"),
    # Gezbir (cluster)
    (1, 7477, None, "claude:unclear_acronym"),
]


def main():
    # Group decisions by file_idx
    by_file = {0: [], 1: []}
    for d in DECISIONS:
        by_file[d[0]].append(d)

    overall_changed = 0
    overall_unflagged = 0
    for fi, spec in enumerate(SPECS):
        with spec["map"].open(newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f, delimiter="\t")
            map_fields = list(rdr.fieldnames or [])
            map_rows = list(rdr)
        with spec["data"].open(newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f, delimiter="\t")
            data_fields = list(rdr.fieldnames or [])
            data_rows = list(rdr)
        changed = 0; unflagged = 0
        for _, idx, new_canon, reason in by_file[fi]:
            if idx >= len(map_rows):
                print(f"skip (out of range): {fi}, {idx}")
                continue
            mrow = map_rows[idx]
            current = mrow.get("canonical_type", "")
            if new_canon and new_canon != current:
                mrow["canonical_type"] = new_canon
                if idx < len(data_rows):
                    data_rows[idx][spec["type_col"]] = new_canon
                changed += 1
            mrow["decided_via"] = "claude_opus_47_in_session"
            mrow["review_reason"] = reason
            mrow["needs_review"] = ""
            unflagged += 1
        with spec["map"].open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=map_fields, delimiter="\t")
            w.writeheader()
            for r in map_rows: w.writerow({k: r.get(k, "") for k in map_fields})
        with spec["data"].open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=data_fields, delimiter="\t")
            w.writeheader()
            for r in data_rows: w.writerow({k: r.get(k, "") for k in data_fields})
        print(f"{spec['map'].name}: {changed} reclassified, {unflagged} total processed (unflagged)")
        overall_changed += changed
        overall_unflagged += unflagged
    print(f"\nTotal: {overall_changed} reclassified, {overall_unflagged} unflagged")


if __name__ == "__main__":
    main()
