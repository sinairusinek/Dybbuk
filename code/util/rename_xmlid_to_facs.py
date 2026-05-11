XML_NS = "http://www.w3.org/XML/1998/namespace"
XML_ID = f"{{{XML_NS}}}id"

def rename_xmlid_to_facs(tree):
    """
    Rename @xml:id -> @facs on every element that has xml:id.
    """
    for el in tree.xpath("//*[@xml:id]", namespaces={"xml": XML_NS}):
        xmlid = el.get(XML_ID)
        if xmlid is None:
            continue
        facs_val = xmlid
        el.set("facs", facs_val)
        del el.attrib[XML_ID]
