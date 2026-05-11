def remove_n_from_lb_only(tree, tei_nsmap):
    """Remove @n from <lb> elements only (do NOT touch <pb>)."""
    lbs = tree.xpath("//tei:lb[@n]", namespaces=tei_nsmap)
    for lb in lbs:
        del lb.attrib["n"]
    print(f"Removed @n from {len(lbs)} <lb> elements.")
