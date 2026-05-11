"""
Speaker name normalization data for Lateiner_Meshumed.

SPEAKER_VARIANTS maps raw speaker text (after stripping XML tags/whitespace)
to a canonical xml:id. PERSON_LIST maps xml:id to display name for <listPerson>.

To adapt for a different play, replace both dicts.
"""

# Scene/act markers that look like speakers but aren't
SCENE_MARKERS = {"IIR", "IR", "SI", "RI", "R.I", "R.II", "II R", "R II S II", "S II"}

# Raw speaker text -> canonical xml:id
SPEAKER_VARIANTS = {
    # מעכיל (Mechel) - tavern keeper, Yakob's brother
    "מעכיל": "mechel",
    "מעכיל.": "mechel",
    "מעכעל.": "mechel",
    "מעכעלע.": "mechel",
    "מעכילע": "mechel",
    "מיכעל.": "mechel",

    # שרה (Sarah) - Mechel's wife
    "שרה": "sarah",
    "שרה.": "sarah",
    "שרה..": "sarah",

    # יוסף / אסיפ (Yosef/Assif) - Yakob's son, the convert
    "יוסף": "yosef",
    "יוסף.": "yosef",
    "יוסעל.": "yosef",
    "יוסיף": "yosef",
    "יוסיף.": "yosef",
    "יוסיף!": "yosef",
    "יוסיףI": "yosef",
    "וסיף.": "yosef",
    "אסיפ": "yosef",
    "אסיפ.": "yosef",
    "אסייפ.": "yosef",
    "יאסיפ.": "yosef",

    # יעקב (Yakob) - merchant, Yosef's father
    "יעקב": "yakob",
    "יעקב.": "yakob",
    "יאקב.": "yakob",

    # ליובאוו / לובאוו (Liubov) - Assif's beloved
    "ליובאוו": "liubov",
    "ליובאוו.": "liubov",
    "לובאוו": "liubov",
    "לובאוו.": "liubov",
    "ליובאָוו": "liubov",
    "ליבאוו.": "liubov",
    "ליוב.": "liubov",

    # ניקיטין (Nikitin) - the Pristav
    "ניקיטין": "nikitin",
    "ניקיטין.": "nikitin",
    "ניקיט": "nikitin",
    "ניקיט.": "nikitin",
    "ניקיטיוו": "nikitin",
    "ניקיטיין.": "nikitin",

    # קאצאפ (Katsap) - Russian peasant
    "קאצאפ": "katsap",
    "קאצאפ.": "katsap",

    # סענדער (Sender) - Mechel & Sarah's son, student
    "סענדער": "sender",
    "סענדער.": "sender",
    "סענדיר.": "sender",

    # איוואן (Ivan) - officer
    "איוואן": "ivan",
    "איוואן.": "ivan",
    "איווין.": "ivan",

    # איוואניוו (Ivanov) - witness
    "איוואניוו.": "ivanov",

    # גרעגואר (Gregor) - officer
    "גרעגואר": "gregor",
    "גרעגואר.": "gregor",

    # ריכטיר (Richter) - judge
    "ריכטיר.": "richter",

    # שפיאן (Shpion) - secret policeman
    "שפיאן": "shpion",
    "שפיאן.": "shpion",

    # סאלד (Soldier)
    "סאלד": "soldat",
    "סאלד.": "soldat",

    # פאליציי / פאליצ / פוליצ (Police)
    "פאליציי": "politsay",
    "פאליציי.": "politsay",
    "פאליצ": "politsay",
    "פאליצ.": "politsay",
    "פוליצ.": "politsay",

    # פאליסמ / פֿאליסמ (Policeman)
    "פאליסמ": "polisman",
    "פאליסמ.": "polisman",
    "פֿאליסמ.": "polisman",
    "פֿאליס": "polisman",

    # גייסט (Ghost/Spirit/Priest)
    "גייסט.": "geistl",
    "גייסטל.": "geistl",

    # פֿאוועל / פֿאוו (Fovel)
    "פֿאוועל": "fovel",
    "פֿאוו": "fovel",
    "פֿאוו.": "fovel",
    "פֿאוויל": "fovel",
    "פֿאוול": "fovel",

    # יוד (Jew)
    "יוד": "yud",
    "יוד.": "yud",

    # קאהר / כהר (Choir)
    "קאהר.": "kohr",
    "כהר": "kohr",
    "כהר -": "kohr",

    # Group speakers
    "אלע.": "ale",
    "אללע": "ale",
    "קינד.": "kind",
    "דועט.": "duet",
    "פֿרויען.": "froyen",
    "ביידע.": "beyde",
    "איינער": "eyner",

    # Placeholder
    "XX": "xx",
}

# Canonical xml:id -> display name for <listPerson>
PERSON_LIST = {
    "mechel": "מעכיל",
    "sarah": "שרה",
    "yosef": "יוסף",
    "yakob": "יעקב",
    "liubov": "ליובאוו",
    "nikitin": "ניקיטין",
    "katsap": "קאצאפ",
    "sender": "סענדער",
    "ivan": "איוואן",
    "ivanov": "איוואניוו",
    "gregor": "גרעגואר",
    "richter": "ריכטיר",
    "shpion": "שפיאן",
    "soldat": "סאלדאט",
    "politsay": "פאליציי",
    "polisman": "פאליסמאן",
    "geistl": "גייסטליכער",
    "fovel": "פֿאוועל",
    "yud": "יוד",
    "kohr": "קאהר",
    "ale": "אלע",
    "kind": "קינד",
    "duet": "דועט",
    "froyen": "פֿרויען",
    "beyde": "ביידע",
    "eyner": "איינער",
    "xx": "XX",
}
