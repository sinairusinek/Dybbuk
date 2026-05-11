import os
from acdh_tei_pyutils.tei import TeiReader
from lxml import etree
from collections import Counter
from pathlib import Path
from io import StringIO


# Define project name: Should match stem of XML file from Transkribus.
project = "Lateiner_Meshumed"

# Define relevant paths
repo_root = Path(__file__).resolve().parents[1]
project_path = Path(repo_root) / "data" / project
processed_image_dir = Path(repo_root) / "data" / project / "images" / "processed"
intermediate_dir = project_path / "text" / "intermediate"
intermediate_dir.mkdir(parents=True, exist_ok=True)

from util import split_images, make_paged_xml, remove_facs_and_type_attrs, rename_xmlid_to_facs, remove_recto_number_lines, remove_n_from_lb_only, remove_pages_without_images, page_children_ab_to_pb_milestones, delete_tag, unwrap_tag   

# Split two-page spreads in half:
# Specify images to exclude (e.g. title page).
exclude = {"0001", "0002", "0003", "0044"}
split_images.split_images(project, exclude, repo_root)

# Manually inspect split images and remove any additional ones that lack content (e.g. img_0013_r.jpg).

paged_xml = make_paged_xml.make_paged_xml(project,project_path)

doc = TeiReader(paged_xml)
tei_nsmap = doc.nsmap  

remove_facs_and_type_attrs.remove_facs_and_type_attrs(doc.tree)
rename_xmlid_to_facs.rename_xmlid_to_facs(doc.tree)
remove_recto_number_lines.remove_recto_number_lines(doc.tree, tei_nsmap)
remove_n_from_lb_only.remove_n_from_lb_only(doc.tree, tei_nsmap)

processed_image_dir = repo_root / "data" / project / "images" / "processed"

remove_pages_without_images.remove_pages_without_images(doc, processed_image_dir)
page_children_ab_to_pb_milestones.page_children_ab_to_pb_milestones(doc, processed_image_dir)

# Remove irrelevant tags
tags_to_unwrap = ["personName", "placeName", "topic", "unclear"]

for tag in tags_to_unwrap:
    # XPath returns _Element objects
    elems = doc.tree.xpath(f"//tei:{tag}", namespaces=doc.nsmap)
    for el in elems:
        unwrap_tag.unwrap_tag(el)
        
xpaths_to_delete = [
    "//tei:_deleted",
    "//tei:hi[contains(@style, 'line-through')]",
]

for xp in xpaths_to_delete:
    elems = doc.tree.xpath(xp, namespaces=doc.nsmap)
    for el in elems:
        delete_tag.delete_tag(el)

# Remove the facsimile section, which is no longer relevant.
for facsimile in doc.tree.xpath("//tei:facsimile", namespaces=tei_nsmap):
    facsimile.getparent().remove(facsimile)

# Write intermediate XML file.
tree = doc.tree.getroottree() if hasattr(doc.tree, "getroottree") else doc.tree
output_path = intermediate_dir / f"{project}_edited.xml"

# TODO: Among other things, figure out marking speakers. I have a couple of ideas.

tree.write(
    output_path,
    encoding="utf-8",
    xml_declaration=True,
    pretty_print=True
)