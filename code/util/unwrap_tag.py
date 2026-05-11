def unwrap_tag(el):
    """
    Remove an lxml element, keeping its text, tail, and child elements.
    """
    parent = el.getparent()
    if parent is None:
        return

    # Insert el.text before el in parent
    if el.text:
        if el.getprevious() is not None:
            # append text to previous sibling's tail
            prev = el.getprevious()
            prev.tail = (prev.tail or "") + el.text
        else:
            # append text to parent.text if first child
            parent.text = (parent.text or "") + el.text

    # Move children to parent at el's position
    for child in list(el):
        el.remove(child)
        parent.insert(parent.index(el), child)

    # Append el.tail to previous sibling or parent
    if el.tail:
        if el.getprevious() is not None:
            prev = el.getprevious()
            prev.tail = (prev.tail or "") + el.tail
        else:
            parent.text = (parent.text or "") + el.tail

    parent.remove(el)
