from pathlib import Path
import warnings
from .delete_tag import delete_tag

def page_children_ab_to_pb_milestones(doc, processed_image_dir, ext=".jpg"):
    tei_ns = doc.nsmap["tei"]
    processed_image_dir = Path(processed_image_dir)

    image_basenames = {
        path.stem
        for path in processed_image_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ext.lower()
    }

    pages = doc.tree.xpath("//tei:page[@facs]", namespaces=doc.nsmap)

    for page in pages:
        page_id = page.attrib["facs"]
        abs_ = list(page)

        if len(abs_) > 2:
            raise RuntimeError(f"Page {page_id} has {len(abs_)} children; expected at most 2.")

        if len(abs_) == 2:
            planned = list(zip(abs_, ["_r", "_l"]))

        elif len(abs_) == 1:
            matches = [name for name in image_basenames if name.startswith(f"{page_id}_")]

            if len(matches) != 1:
                warnings.warn(
                    f"{page_id}: expected exactly 1 matching split image, found {len(matches)}. "
                    "Leaving page content unchanged."
                )
                delete_tag(page)
                continue

            match = matches[0]
            if match.endswith("_l"):
                planned = [(abs_[0], "_l")]
            elif match.endswith("_r"):
                planned = [(abs_[0], "_r")]
            else:
                warnings.warn(
                    f"{page_id}: matching image does not end in _l or _r ({match}). "
                    "Leaving page content unchanged."
                )
                delete_tag(page)
                continue

        else:
            planned = []

        for ab, suffix in planned:
            new_facs = f"{page_id}{suffix}"

            if new_facs not in image_basenames:
                warnings.warn(
                    f"{page_id}: expected image {new_facs}{ext} not found. "
                    "Leaving this <ab> unchanged."
                )
                continue

            parent = ab.getparent()
            idx = parent.index(ab)

            pb = doc.tree.getroottree().getroot().makeelement(f"{{{tei_ns}}}pb")

            pb.attrib.update(page.attrib)
            pb.attrib.update(ab.attrib)

            # replace facs with suffixed version
            pb.attrib["facs"] = new_facs

            parent.insert(idx, pb)

            if ab.text:
                pb.tail = (pb.tail or "") + ab.text

            insert_pos = parent.index(pb) + 1
            for child in list(ab):
                ab.remove(child)
                parent.insert(insert_pos, child)
                insert_pos += 1

            if ab.tail:
                if insert_pos - 1 > parent.index(pb):
                    parent[insert_pos - 1].tail = (parent[insert_pos - 1].tail or "") + ab.tail
                else:
                    pb.tail = (pb.tail or "") + ab.tail

            parent.remove(ab)

        delete_tag(page)