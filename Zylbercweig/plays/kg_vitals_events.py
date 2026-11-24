"""Step 9c: turn the vitals into birth / death / burial EVENTS.

The bio layer records a person's vitals as three person→place edges (born_in,
died_in, buried_in). D–G separately extracted death and burial as EVENTS, which
hold what an edge cannot: the date, the place and the other people involved.
The same fact was being carried twice, in two shapes.

This module replaces the edges with events. It runs after kg_bio, over the
graph that layer has already built, and it is driven by the reconciliation in
`extraction_v2/qa/vitals_events_reconciliation.tsv`
(built by `extraction_v2/reconcile_vitals_events.py` — read that first).

What happens to each of the four reconciliation verdicts:

  BOTH_AGREE   the D–G event is IMPORTED into the graph and the edge retired.
               The import is the whole point: the D–G events live in
               extraction_v2/defg/, not in the KG, so retiring an edge without
               importing its event deletes the fact. (That is exactly what an
               earlier version of this module did — 103 people silently lost
               their death or burial. Never retire an edge whose replacement
               is not in `g` at the end of the run; the self-check below
               enforces it.)
  EDGE_ONLY    267 entries D–G never extracted. The edge is CARRIED OVER as an
               event built from the vitals, because dropping it would lose the
               fact outright. (Sinai, 2026-11-24.) Birth is always this case —
               no birth event type was ever extracted.
  EVENT_ONLY   the D–G event is imported too: it was never in the KG either.
  BOTH_DIFFER  167 genuine conflicts. LEFT ALONE — both the edge and the event
               stay, and the pair is a team assignment for the meeting around
               2026-12-07. Nothing here may be resolved automatically: the two
               sources disagree on evidence, and picking one silently would
               destroy the disagreement that makes it worth reviewing.

So the edges that survive a run are exactly the ones under review, plus any
the reconciliation could not key.

    python3.11 kg_vitals_events.py --dry-run    # counts only, no writes
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

import plays_common as pc

csv.field_size_limit(sys.maxsize)

HERE = Path(__file__).resolve().parent
EV2 = HERE.parent / "extraction_v2"
RECON = EV2 / "qa" / "vitals_events_reconciliation.tsv"
DG_EVENTS = EV2 / "defg" / "events.tsv"
DG_PARTS = EV2 / "defg" / "event_participants.tsv"

# edge type -> the event kind it becomes
EDGE_EVENT = {"born_in": "birth", "died_in": "death", "buried_in": "burial"}
EVENT_EDGE = {v: k for k, v in EDGE_EVENT.items()}

# the participant role on a vital event. NOT `victim`: that term was applied to
# every death, but fewer than half show any violence in the text, and it also
# reached arrest, illness and military service. `subject` = the person the
# event is about; violence is a property of the event, not of the role.
SUBJECT_ROLE = "subject"


def load_dg_events() -> tuple[dict, dict]:
    """The D–G death / burial events, keyed (entry_id, kind), + participants.

    These live in extraction_v2/defg/ and have never been part of the KG.
    The migration has to bring them in; it cannot point at them in place.
    """
    if not DG_EVENTS.exists():
        raise SystemExit(f"missing {DG_EVENTS}")
    by_key: dict[tuple[str, str], list] = defaultdict(list)
    with DG_EVENTS.open(encoding="utf-8", newline="") as fh:
        for r in csv.DictReader(fh, delimiter="\t"):
            if r["event_type"] in ("death", "burial") and r["entry_id"].strip():
                by_key[(r["entry_id"], r["event_type"])].append(r)
    parts: dict[str, list] = defaultdict(list)
    if DG_PARTS.exists():
        with DG_PARTS.open(encoding="utf-8", newline="") as fh:
            for r in csv.DictReader(fh, delimiter="\t"):
                parts[r["ekey"]].append(r)
    return by_key, parts


def load_reconciliation() -> dict[tuple[str, str], dict]:
    """(entry_id, kind) -> its reconciliation row."""
    if not RECON.exists():
        raise SystemExit(
            f"missing {RECON}\n"
            f"Run extraction_v2/reconcile_vitals_events.py --tsv first: the "
            f"migration is driven by its verdicts and must not guess them.")
    with RECON.open(encoding="utf-8", newline="") as fh:
        return {(r["entry_id"], r["kind"]): r
                for r in csv.DictReader(fh, delimiter="\t")}


def _emit_event(g, ev_id, kind, person, *, place_surface="", place_id="",
                date_start="", date_end="", date_precision="", entry="",
                evidence="", model="", confidence="", note="",
                fact_ids="", match_status="", extra_people=()):
    """One vital event: the node, its subject, its place, anyone else named."""
    g.add_node(ev_id, node_type="event", label_english=kind,
               match_status=match_status or "matched", notes=note,
               source_layer="bio",
               attrs=json.dumps({"event_type": kind}, ensure_ascii=False))
    common = dict(provenance_person_id=entry, provenance_fact_ids=fact_ids,
                  evidence_sentence=evidence, extraction_model=model,
                  confidence=confidence, match_status=match_status or "matched",
                  review_status="auto", source_layer="bio")
    g.add_edge(source_id=ev_id, target_id=person, edge_type="participant",
               role_detail=SUBJECT_ROLE, date_start=date_start,
               date_end=date_end, date_precision=date_precision, **common)
    # the place: an id when the edge already had one, else the D–G surface,
    # resolved through the same mint path every other layer uses
    target = place_id
    if not target and place_surface:
        target = g.resolve_surface("place", place_surface, "bio",
                                   entry, evidence, node_type="place",
                                   label_yiddish=place_surface,
                                   match_status="unmatched",
                                   notes="vitals event place",
                                   source_layer="bio")
    if target:
        g.add_edge(source_id=ev_id, target_id=target, edge_type="held_in",
                   date_start=date_start, date_precision=date_precision,
                   **common)
    # People the text names alongside the subject are what an event can hold
    # and an edge cannot — but D–G's `person_key` is a MENTION id, not a
    # people_db id, so none of them resolves to a node yet (checked: 0 of
    # 2,000). Emitting them produced 1,299 dangling edges. They arrive with
    # step 9 (surname ER); until then the names are kept on the event rather
    # than pointed at nodes that do not exist.
    others = [((pr.get("person") or "").strip(), (pr.get("role") or "").strip())
              for pr in extra_people
              if (pr.get("person") or "").strip()]
    if others:
        g.nodes[ev_id]["notes"] = " · ".join(filter(None, [
            g.nodes[ev_id].get("notes", ""),
            "also named: " + ", ".join(
                f"{nm} ({rl})" if rl else nm for nm, rl in others[:8])]))


def add_vitals_events(g, dry_run: bool = False) -> Counter:
    """Convert the vitals edges in `g` into events. Returns a stats Counter."""
    recon = load_reconciliation()
    dg_events, dg_parts = load_dg_events()
    stats: Counter = Counter()
    imported: set[str] = set()

    vitals = [e for e in g.edges if e.get("edge_type") in EDGE_EVENT]
    stats["vitals_edges_before"] = len(vitals)

    # Group by (person, ENTRY, kind). Grouping by person alone is wrong: a
    # multi-entry hub such as person:1409 (דוד ברוין) merges two Lexicon
    # entries, each carrying its own death — New York 1940 and Los Angeles
    # 1968. The reconciliation is keyed by entry, so the grouping must be too,
    # or one entry's verdict silently decides the other entry's fact.
    by_subject: dict[tuple[str, str, str], list] = defaultdict(list)
    for e in vitals:
        by_subject[(e["source_id"], e.get("provenance_person_id", ""),
                    EDGE_EVENT[e["edge_type"]])].append(e)

    retire = []
    for (person, entry, kind), edges in sorted(by_subject.items()):
        row = recon.get((entry, kind))
        verdict = row["verdict"] if row else ""

        if verdict == "BOTH_DIFFER":
            # a team assignment, not a migration case — leave both sides as they
            # are so the conflict stays visible to whoever reviews it
            stats[f"{kind}:left_for_review"] += 1
            continue

        if verdict in ("BOTH_AGREE", "EVENT_ONLY"):
            # Import the D–G event, THEN retire the edge. Doing only the second
            # half deletes the fact: these events are not otherwise in the KG.
            for dg in dg_events.get((entry, kind), []):
                ev_id = f"event:{kind}:{entry}:{dg['ekey'].split(':')[-1]}"
                if not dry_run:
                    _emit_event(g, ev_id, kind, person,
                                place_surface=dg.get("place", ""),
                                date_start=dg.get("date_start", ""),
                                date_end=dg.get("date_end", ""),
                                entry=entry,
                                evidence=dg.get("evidence", ""),
                                model="defg", confidence="high",
                                note=f"imported from D–G {dg['ekey']}",
                                extra_people=dg_parts.get(dg["ekey"], []))
                imported.add(ev_id)
            if (entry, kind) not in dg_events:
                # the reconciliation says there is an event but we cannot find
                # it — do NOT retire the edge on a promise we cannot keep
                stats[f"{kind}:event_missing_edge_kept"] += 1
                continue
            retire.extend(edges)
            stats[f"{kind}:retired_in_favour_of_event"] += 1
            continue

        # EDGE_ONLY, or no reconciliation row (birth is always here): the
        # fact exists only as an edge, so carry it over rather than lose it
        for e in edges:
            # keyed by entry as well: two entries on one hub are two events
            ev_id = f"event:{kind}:{person.split(':', 1)[-1]}:{entry}"
            if not dry_run:
                _emit_event(g, ev_id, kind, person,
                            place_id=e["target_id"],
                            date_start=e.get("date_start", ""),
                            date_end=e.get("date_end", ""),
                            date_precision=e.get("date_precision", ""),
                            entry=entry,
                            evidence=e.get("evidence_sentence", ""),
                            model=e.get("extraction_model", ""),
                            confidence=e.get("confidence", ""),
                            fact_ids=e.get("provenance_fact_ids", ""),
                            match_status=e.get("match_status", ""),
                            note=f"carried over from the {e['edge_type']} "
                                 f"vitals edge; D–G did not extract it")
            imported.add(ev_id)
            retire.append(e)
            stats[f"{kind}:carried_over_as_event"] += 1

    if not dry_run:
        drop = {id(e) for e in retire}
        g.edges[:] = [e for e in g.edges if id(e) not in drop]

        # SELF-CHECK. Every person who had a vital must still have it, either
        # as a surviving edge (under review) or as an event. An earlier version
        # retired 860 edges "in favour of" D–G events that were never in the
        # graph, and 103 people silently lost their death or burial. Losing a
        # fact must stop the build, not be discovered later in a diff.
        had = {e["source_id"] for e in vitals}
        keeps = {e["source_id"] for e in g.edges
                 if e.get("edge_type") in EDGE_EVENT}
        subjects = {e["target_id"] for e in g.edges
                    if e.get("edge_type") == "participant"
                    and str(e.get("source_id", "")).startswith("event:")}
        lost = had - keeps - subjects
        if lost:
            raise SystemExit(
                f"step 9c would LOSE the vitals of {len(lost)} people "
                f"(e.g. {sorted(lost)[:5]}).\n"
                f"An edge was retired without its replacement event reaching "
                f"the graph. Fix the import before running again — do not "
                f"commit a graph built from this state.")
        stats["people_checked"] = len(had)

        # (2) nothing this step emits may point at a node that does not exist.
        # D–G's `person_key` is a MENTION id, not a people_db id, so naively
        # emitting co-participants produced 1,299 dangling edges. The first
        # version of this check tested only (1) and missed it.
        mine = [e for e in g.edges
                if str(e.get("source_id", "")).startswith("event:")
                and e.get("source_layer") == "bio"]
        dangling = [e for e in mine
                    if e["source_id"] not in g.nodes
                    or e["target_id"] not in g.nodes]
        if dangling:
            raise SystemExit(
                f"step 9c would leave {len(dangling)} DANGLING edges "
                f"(e.g. {[(e['source_id'], e['edge_type'], e['target_id']) for e in dangling[:3]]}).\n"
                f"Every endpoint must be a node in the graph.")

        # (3) the rows under review must survive untouched: they are a team
        # assignment, and resolving one by deleting it is the worst outcome.
        # Count the GROUPS left, not the edges: a person may carry more than
        # one edge of a kind. person:1409 (דוד ברוין) has two died_in — New
        # York 1940 and Los Angeles 1968 — which is two people merged under
        # one id, or two entries sharing a node. A real data problem, and it
        # belongs in the review queue, not in this check's arithmetic.
        under_review = {k for k, r in recon.items()
                        if r["verdict"] == "BOTH_DIFFER"}
        left_groups = {(e.get("provenance_person_id", ""),
                        EDGE_EVENT[e["edge_type"]])
                       for e in g.edges if e.get("edge_type") in EDGE_EVENT}
        if left_groups != under_review:
            extra = sorted(left_groups - under_review)[:3]
            missing = sorted(under_review - left_groups)[:3]
            raise SystemExit(
                f"step 9c left the wrong vitals edges: {len(left_groups)} "
                f"entry/kind groups survive, {len(under_review)} are "
                f"BOTH_DIFFER.\n  retired but under review: {missing}"
                f"\n  kept but not under review: {extra}")
        stats["left_for_review_groups"] = len(left_groups)
        stats["left_for_review_edges"] = len(
            [e for e in g.edges if e.get("edge_type") in EDGE_EVENT])

    stats["edges_retired"] = len(retire)
    stats["vitals_edges_after"] = len(vitals) - len(retire)
    stats["events_created"] = len(imported)
    return stats


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would change, write nothing")
    args = ap.parse_args()
    raise SystemExit(
        "kg_vitals_events is a layer module: it runs inside build_kg.py over "
        "the graph kg_bio has built, not standalone.\n"
        "Dry-run the whole build instead:\n"
        "    python3.11 build_kg.py --dry-run\n"
        f"Reconciliation driving it: {RECON}")


if __name__ == "__main__":
    raise SystemExit(main())
