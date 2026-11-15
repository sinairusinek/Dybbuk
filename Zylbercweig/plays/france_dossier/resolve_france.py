#!/usr/bin/env python3.11
"""France-only post-pass over itinerary/france_itineraries.json.

The shared resolver misses French spellings the gazetteer never attested
(Lyon, Vittel, the camps) and OCR'd forms of Paris. They are resolved here only
for stations Gemini put in France, so the shared tables stay untouched.
QIDs are given only where certain; otherwise "" with coordinates."""
import json, re, unicodedata
from pathlib import Path

H = Path(__file__).resolve().parent
J = H.parent / "itinerary" / "france_itineraries.json"
POINTS = re.compile(r"[֑-ׇ]")

SUP = {  # unpointed spelling -> (qid, label, lat, lon, kind)
    "פאריז": ("Q90", "Paris", 48.857, 2.352, "s"), "פאדיז": ("Q90", "Paris", 48.857, 2.352, "s"),
    "פארין": ("Q90", "Paris", 48.857, 2.352, "s"), "פאריזש": ("Q90", "Paris", 48.857, 2.352, "s"),
    "פאריזער": ("Q90", "Paris", 48.857, 2.352, "s"), "פאריס": ("Q90", "Paris", 48.857, 2.352, "s"),
    "ליאן": ("Q456", "Lyon", 45.764, 4.836, "s"), "יאן": ("Q456", "Lyon", 45.764, 4.836, "s"),
    "מארסעל": ("Q23482", "Marseille", 43.296, 5.370, "s"), "מארסיי": ("Q23482", "Marseille", 43.296, 5.370, "s"),
    "ניצא": ("Q33959", "Nice", 43.710, 7.262, "s"), "ניס": ("Q33959", "Nice", 43.710, 7.262, "s"),
    "שטראסבורג": ("Q6602", "Strasbourg", 48.573, 7.752, "s"), "סטראסבורג": ("Q6602", "Strasbourg", 48.573, 7.752, "s"),
    "נאנסי": ("Q40898", "Nancy", 48.692, 6.184, "s"), "נאנסי": ("Q40898", "Nancy", 48.692, 6.184, "s"),
    "מעץ": ("Q22690", "Metz", 49.120, 6.177, "s"),
    "בארדא": ("Q1479", "Bordeaux", 44.838, -0.579, "s"), "באָרדאָ": ("Q1479", "Bordeaux", 44.838, -0.579, "s"),
    "טולוז": ("Q7880", "Toulouse", 43.605, 1.444, "s"), "טולוזע": ("Q7880", "Toulouse", 43.605, 1.444, "s"),
    "ליל": ("Q648", "Lille", 50.629, 3.057, "s"), "לימאזש": ("Q45656", "Limoges", 45.834, 1.262, "s"),
    "דראנסי": ("Q201348", "Drancy", 48.923, 2.445, "s"), "דראנסיי": ("Q201348", "Drancy", 48.923, 2.445, "s"),
    "ווישי": ("", "Vichy", 46.128, 3.426, "s"), "וויטעל": ("", "Vittel", 48.202, 5.947, "s"),
    "גורס": ("", "Gurs (camp)", 43.276, -0.735, "s"), "פיטיוויער": ("", "Pithiviers (camp)", 48.172, 2.252, "s"),
    "באן-לא ראלאנד": ("", "Beaune-la-Rolande (camp)", 48.071, 2.432, "s"),
    "באן לא ראלאנד": ("", "Beaune-la-Rolande (camp)", 48.071, 2.432, "s"),
    "ריוועזאלט": ("", "Rivesaltes (camp)", 42.769, 2.874, "s"),
    "עלזאס לאטרינגען": ("", "Alsace-Lorraine", 48.6, 7.0, "r"), "עלזאס-לאטרינגען": ("", "Alsace-Lorraine", 48.6, 7.0, "r"),
    "דרום-פראנקרייך": ("", "Southern France", 43.6, 3.9, "r"), "פרייער זאנע": ("", "Unoccupied zone", 45.8, 2.8, "r"),
    "פריי זאנע": ("", "Unoccupied zone", 45.8, 2.8, "r"),
    "פראנצויזישער פראווינץ": ("", "French provinces", 46.6, 2.4, "r"),
    "פראנצעזישער פראווינץ": ("", "French provinces", 46.6, 2.4, "r"),
    "פראנקרייך": ("Q142", "France", 46.6, 2.4, "r"), "France": ("Q142", "France", 46.6, 2.4, "r"),
    "פרייער קאנע": ("", "Unoccupied zone", 45.8, 2.8, "r"),
    "בית עולם באניע": ("", "Bagneux (cemetery)", 48.796, 2.308, "s"),
}


def nm(s):
    s = POINTS.sub("", unicodedata.normalize("NFD", s or "")).replace("־", "-")
    return re.sub(r"[„“\"'()\[\],.]", " ", s).split(",")[0].strip()


def fix(st):
    if st.get("label_en") or st.get("country") != "France":
        return False
    p = re.sub(r"\s+", " ", nm(st["place"]))
    cands = [p] + [re.sub(r"^לאגער ", "", p)] + p.split()
    for c in cands:
        c2 = re.sub(r"ער$", "", c)
        for k in (c, c2):
            if k in SUP:
                q, lab, lat, lon, kind = SUP[k]
                st.update(qid=q or lab, label_en=lab, lat=lat, lon=lon,
                          res_status="settlement_france" if kind == "s" else "region_france")
                return True
    return False


if __name__ == "__main__":
    D = json.load(open(J, encoding="utf-8"))
    n = left = 0
    miss = {}
    for it in D:
        for st in it["stations"]:
            if fix(st):
                n += 1
            elif st.get("country") == "France" and not st.get("label_en"):
                left += 1
                miss[nm(st["place"])] = miss.get(nm(st["place"]), 0) + 1
    json.dump(D, open(J, "w", encoding="utf-8"), ensure_ascii=False)
    print(f"France post-pass: resolved {n}, still unresolved {left}")
    print(sorted(miss.items(), key=lambda x: -x[1])[:40])
