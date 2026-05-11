def remove_facs_and_type_attrs(tree):
    """Remove all @facs and @type attributes from every element."""
    for el in tree.xpath("//*[@facs or @type]"):
        if "facs" in el.attrib:
            del el.attrib["facs"]
        if "type" in el.attrib:
            del el.attrib["type"]
