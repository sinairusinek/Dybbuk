"""
Transform preprocessed Yiddish play XML into DraCor-compatible TEI.

Usage: python dracor_transform.py

Input:  data/{project}/text/intermediate/{project}_edited.xml
Output: data/{project}/text/intermediate/{project}_dracor.xml
"""

import logging
from pathlib import Path
from collections import Counter
from acdh_tei_pyutils.tei import TeiReader

from util.mark_speakers import parse_body, build_dracor_tree

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

project = "Lateiner_Meshumed"
repo_root = Path(__file__).resolve().parents[1]
project_path = repo_root / "data" / project

input_path = project_path / "text" / "intermediate" / f"{project}_edited.xml"
output_path = project_path / "text" / "intermediate" / f"{project}_dracor.xml"

# Load
doc = TeiReader(str(input_path))
logger.info(f"Loaded {input_path}")

# Parse
segments = parse_body(doc)

# Stats
sp_count = sum(1 for s in segments if s["type"] == "sp")
stage_count = sum(1 for s in segments if s["type"] == "stage")
act_starts = [s for s in segments if s["type"] == "act_start"]
speaker_counts = Counter(s["who"] for s in segments if s["type"] == "sp")

logger.info(f"Parsed {sp_count} speeches, {stage_count} stage directions, {len(act_starts)} acts")
logger.info("Speeches per character:")
for char_id, count in speaker_counts.most_common():
    logger.info(f"  {char_id}: {count}")

# Build DraCor TEI
tree = build_dracor_tree(segments, project)

# Write
tree.write(str(output_path), encoding="utf-8", xml_declaration=True, pretty_print=True)
logger.info(f"Written to {output_path}")

# Validate
TEI_NS = "http://www.tei-c.org/ns/1.0"
XML_NS = "http://www.w3.org/XML/1998/namespace"
nsmap = {"tei": TEI_NS, "xml": XML_NS}

persons = tree.xpath("//tei:person/@xml:id", namespaces=nsmap)
sp_whos = tree.xpath("//tei:sp/@who", namespaces=nsmap)
bad_refs = [w for w in sp_whos if w.lstrip("#") not in persons]
if bad_refs:
    logger.warning(f"Bad @who references: {bad_refs}")
else:
    logger.info(f"All {len(sp_whos)} @who references valid ({len(persons)} characters in listPerson)")

acts = tree.xpath("//tei:div[@type='act']", namespaces={"tei": TEI_NS})
for act in acts:
    scenes = act.xpath("tei:div[@type='scene']", namespaces={"tei": TEI_NS})
    sp_in_act = act.xpath(".//tei:sp", namespaces={"tei": TEI_NS})
    logger.info(f"  Act {act.get('n')}: {len(scenes)} scene(s), {len(sp_in_act)} speeches")
