import re
from pathlib import Path

def make_paged_xml(project: str, project_path: Path) -> str:
    """
    Convert <pb .../> tags into paired <page ...>...</page> tags.
    """
    input_path = project_path / "text" / "raw" / f"{project}.xml"

    with open(input_path, "r", encoding="utf-8") as f:
        txt = f.read()

    # 1. <pb .../> → <page ...>
    txt = re.sub(r"<pb(\s[^/>]*)/>", r"<page\1>", txt)

    # 2. Insert </page> before every <page>
    txt = re.sub(r"(?=<page\b)", r"</page>\n", txt)

    # 3. Remove the very first injected </page>
    txt = txt.replace("</page>\n<page", "<page", 1)

    # 4. Close the final page before </div>
    txt = re.sub(r"\n?\s*</div>", r"\n</page>\n</div>", txt, count=1)

    # Optional: write intermediate file for inspection
    output_path = project_path / "text" / "intermediate" / f"{project}_paged.xml"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(txt)

    return txt