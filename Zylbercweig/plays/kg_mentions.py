"""KG person-mentions layer — vol-3 in-text person mentions as person<->person edges.

Step 3 of the Colab-extraction -> KG integration.

Source
  people/people_mentions_extracted.tsv   4,393 mentions from the vol-3 "III"
                                         schema: host entry, mention name,
                                         gender, person_description, relation
                                         (a Yiddish sentence about how the
                                         mentioned person relates to the
                                         subject — 2,982 distinct strings)

Typing: Zylbercweig_extraction/"categorizing relations.xlsx" was meant to be
the vocabulary but classifies only 12 strings, so edges are typed by a
keyword lexicon over the relation sentence (RULES below; first hit wins,
default associated_with).  The matched rule name goes to role_detail and the
full sentence to evidence_sentence so the typing can be re-done later.

Target resolution (mention name -> node), in order:
  1. people/derived_mention_alignments.tsv  surface -> db_id (RA-validated chain)
  2. unambiguous normalized match against a lexicon subject heading
     (kg/entry_index.tsv) -> that subject's node
  3. minted person:UP-nnnn keyed on the normalized surface (shared with the
     plays layer's unlinked-person mint)
"""
from __future__ import annotations

import json
import re
from collections import Counter

import plays_common as pc

MENTIONS_TSV = pc.PEOPLE_DIR / "people_mentions_extracted.tsv"
DERIVED_TSV = pc.PEOPLE_DIR / "derived_mention_alignments.tsv"
EXTRACTION_MODEL = "colab_extraction_III_mentions"

# (edge_type, rule_name, regex over the diacritic-stripped sentence)
# Patterns are written against pc.norm_yiddish output: diacritics stripped,
# FINAL LETTERS UN-FINALIZED (ן->נ, ם->מ, ף->פ, ץ->צ, ך->כ), punctuation -> space.
RULES = [
    ("family_of", "parent", r"\bפאטער\b|\bמוטער\b|\bעלטערנ\b|\bטאטע\b|\bמאמע\b"),
    ("family_of", "child", r"\bזונ\b|\bטאכטער\b|\bקינד\b|\bקינדער\b(?! ראלנ)"),
    ("family_of", "sibling", r"\bברודער\b|\bשוועסטער\b"),
    ("family_of", "spouse", r"\bפרוי\b|\bווייב\b|\bמאנ\b|פארהייראט|\bחתונה\b|געהייראט|\bאלמנה\b|\bאלמנ\b"),
    ("family_of", "in_law", r"\bשוואגער\b|\bשוועגערינ\b|\bשווער\b|\bשוויגער\b|\bאיידעמ\b|\bשנור\b"),
    ("family_of", "relative", r"\bפעטער\b|\bמומע\b|\bקרוב\b|\bקרובה\b|\bפלימעניק\b|\bאייניקל\b|\bזיידע\b|\bבאבע\b|\bמשפחה\b"),
    ("studied_with", "teacher_student", r"\bלערער\b|\bלערערינ\b|\bתלמיד\b|\bתלמידה\b|געלערנט ביי|שטודירט ביי|\bשילער\b"),
    ("wrote_about", "biography", r"ביאגראפיע|אפגעשאצט|רעצענזיע|געשריבנ וועגנ|אפגעדרוקט|פארעפנטלעכט|כאראקטעריסטיק|\bמאנאגראפיע\b"),
    ("performed_work_of", "author_actor", r"געשפילט אינ (?:זיינ|איר|דעמ סוביעקטס|די סוביעקטס|זיינע|אירע)(?: א| די| דער)? ?(?:פיעסע|פיעסעס|דראמע|קאמעדיע|אפערעטע|ווערק|מעלאדראמע)"),
    ("performed_with", "co_performer", r"געשפילט (?:מיט|צוזאמענ|אינאיינעמ)|מיטגעשפילט|מיט (?:אימ|איר|דעמ סוביעקט|דער סוביעקט) געשפילט|מיטגליד (?:פונ|אינ) (?:זיינ|איר) טרופע|אינ (?:זיינ|איר) טרופע|אינ (?:זיינ|איר) טעאטער|אויפגעטראטנ מיט|געשפילט אינ (?:זיינ|איר) (?:אנסאמבל|קאמפאניע)|געגאסטראלירט מיט|באטייליקט [^.]*מיט (?:אימ|איר)\\b|\\bפארטנער|שפילט מיט (?:אימ|איר)|מיטגעזונגענ|מיטגליד (?:אינ|פונ) דער זעלבער"),
    ("performed_work_of", "author_performer", r"(?:געזונגענ|זינגענ|געשפילט|אויפגעפירט) [^.]*(?:זיינע|אירע) (?:לידער|פיעסעס|ווערק)"),
    ("directed_by", "direction", r"\bרעזשי\b|\bרעזשיסער\b|אונטער (?:דער|זיינ|איר) (?:לייטונג|דירעקציע|פארוואלטונג)|\bדירעקטאר\b"),
    ("co_member_of", "co_member", r"\bמיטגליד\b|אנגעשלאסנ אינ|\bאינ דער (?:זעלבער|זעלביקער) טרופע"),
    ("colleague_of", "colleague", r"\bקאלעגע\b|\bחבר\b|\bחברטע\b|\bפריינד\b|\bמיטארבעטער\b"),
]
_RULES = [(et, rn, re.compile(rx)) for et, rn, rx in RULES]


def classify(sentence: str) -> tuple[str, str]:
    s = pc.norm_yiddish(sentence)
    for et, rn, rx in _RULES:
        if rx.search(s):
            return et, rn
    return "associated_with", ""


def add_mentions_layer(g, labels, entry_index: dict[str, dict]) -> dict:
    from build_kg import _norm_name

    people, *_ = labels
    derived = {}
    for r in pc.read_tsv(DERIVED_TSV):
        surf = (r.get("mention_surface") or "").strip()
        if surf and r.get("db_id") and r.get("db_status", "ok") in ("ok", ""):
            derived.setdefault(surf, r["db_id"])
    # unambiguous heading index over lexicon subjects
    head_idx: dict[str, str | None] = {}
    for e in entry_index.values():
        k = _norm_name(e["heading"])
        if not k:
            continue
        head_idx[k] = None if k in head_idx and head_idx[k] != e["node_id"] else e["node_id"]
    stats: Counter = Counter()
    seen: set[tuple] = set()

    for m in pc.read_tsv(MENTIONS_TSV):
        host = entry_index.get(m["host_person_id"])
        name = (m.get("name") or "").strip()
        if not host or not name:
            stats["dropped_no_host_or_name"] += 1
            continue
        if name.endswith("*"):
            # anonymous relative ("פֿאָטער*" = the subject's father): one node
            # per host, never shared across entries, never sent to review
            target = g.mint("person:ANON", f"{host['node_id']}|{pc.norm_yiddish(name)}",
                            node_type="person", label_yiddish=name,
                            match_status="unmatched", source_layer="mentions",
                            notes=f"anonymous relative of {host['node_id']}",
                            attrs=json.dumps({"gender": m.get("gender", ""),
                                              "description": m.get("person_description", "")},
                                             ensure_ascii=False))
            status, conf = "unmatched", "low"
            stats["target_anonymous"] += 1
        elif name in derived:
            db_id = derived[name]
            target = f"person:{db_id}"
            p = people.get(db_id, {})
            g.add_node(target, node_type="person",
                       label_yiddish=p.get("hebname") or name,
                       label_english=p.get("english", ""),
                       ext_ref_type="people_db", ext_ref_id=db_id,
                       match_status="matched", source_layer="mentions")
            status, conf = "matched", "high"
            stats["target_derived_alignment"] += 1
        else:
            nid = head_idx.get(_norm_name(name))
            if nid:
                target, status, conf = nid, "matched", "medium"
                stats["target_heading_match"] += 1
            else:
                target = g.resolve_surface(
                    "person", name, "mentions", m["mention_id"],
                    m.get("relation", ""), node_type="person", label_yiddish=name,
                    match_status="unmatched", source_layer="mentions",
                    attrs=json.dumps({"gender": m.get("gender", ""),
                                      "description": m.get("person_description", "")},
                                     ensure_ascii=False))
                if not target:
                    stats["target_not_entity"] += 1
                    continue
                if target.startswith("person:UP"):
                    status, conf = "unmatched", "low"
                    stats["target_minted"] += 1
                else:
                    status, conf = "matched", "high"
                    stats["target_adjudicated"] += 1
        if target == host["node_id"]:
            stats["dropped_self_reference"] += 1
            continue
        sent = (m.get("relation") or "").strip()
        etype, rule = classify(sent)
        key = (host["node_id"], target, etype, sent)
        if key in seen:
            stats["duplicate"] += 1
            continue
        seen.add(key)
        g.add_edge(source_id=host["node_id"], target_id=target, edge_type=etype,
                   role_detail=rule, character="",
                   date_start="", date_end="", date_precision="",
                   event_id="", production_key="",
                   provenance_person_id=m["host_person_id"],
                   provenance_fact_ids=m["mention_id"],
                   evidence_sentence=sent or m.get("person_description", ""),
                   extraction_model=EXTRACTION_MODEL, confidence=conf,
                   match_status=status, review_status="auto",
                   source_layer="mentions")
        stats[f"edge:{etype}"] += 1
    return dict(sorted(stats.items()))
