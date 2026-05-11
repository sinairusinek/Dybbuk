import re
DIGITS_ONLY_RE = re.compile(r"^\s*\d+\s*$")

def remove_recto_number_lines(tree, tei_nsmap):
    """
    Remove <lb> elements where @n="N001" AND the rest of that line is only digits.
    In typical TEI like: <lb n="N001"/>170719
    the '170719' is in lb.tail.
    """
    lbs = tree.xpath("//tei:lb[@n='N001']", namespaces=tei_nsmap)

    removed = 0
    for lb in lbs:
        tail = lb.tail or ""
        if not DIGITS_ONLY_RE.match(tail):
            continue  # don't remove if there is anything other than digits/whitespace

        parent = lb.getparent()
        if parent is None:
            continue

        # Remove the digits too
        lb.tail = ""

        # Remove lb safely
        parent.remove(lb)
        removed += 1

    print(f"Removed {removed} recto-number <lb n='N001'> lines.")
