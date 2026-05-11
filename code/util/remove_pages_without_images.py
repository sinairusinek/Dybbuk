from pathlib import Path


def remove_pages_without_images(doc, processed_image_dir, prefix="img_", ext=".jpg"):
    processed_image_dir = Path(processed_image_dir)

    base_images = {
        path.stem[:-2] if path.stem.endswith(("_l", "_r")) else path.stem
        for path in processed_image_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ext.lower()
    }

    pages = doc.tree.xpath("//tei:page[@facs]", namespaces=doc.nsmap)

    removed = 0

    for page in pages:
        facs = page.attrib["facs"]
        if facs.startswith(prefix) and facs not in base_images:
            print(f"Removing page with facs={facs}")
            page.getparent().remove(page)
            removed += 1

    print(f"Removed {removed} pages without images")