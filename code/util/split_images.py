from PIL import Image
import sys
from pathlib import Path

def split_images(project, exclude, repo_root):
    """
    Split two-page spreads in half, saving left and right separately, excluding specified images.
    """

    # Set up paths
    image_dir = Path(repo_root) / "data" / project / "images"
    raw_dir = image_dir / "raw" 
    processed_dir = image_dir / "processed" 

    processed_dir.mkdir(parents=True, exist_ok=True)

    # Safety check: abort if processed directory is not empty
    if any(processed_dir.iterdir()):
        print(f"Skipping image split: {processed_dir} is not empty.")
        return

    # Add more suffixes if needed.
    valid_suffixes = {".jpg"}

    for img_path in sorted(raw_dir.iterdir()):
        if not img_path.is_file() or img_path.suffix.lower() not in valid_suffixes:
            continue

        stem = img_path.stem  # e.g. "img_0001"
        parts = stem.split("_")
        num = parts[-1] if parts else stem

        if num in exclude:
            print(f"Skipping excluded file: {img_path.name}")
            continue

        with Image.open(img_path) as im:
            width, height = im.size
            midpoint = width // 2

            left = im.crop((0, 0, midpoint, height))
            right = im.crop((midpoint, 0, width, height))

            left_name = f"{stem}_l{img_path.suffix.lower()}"
            right_name = f"{stem}_r{img_path.suffix.lower()}"

            left.save(processed_dir / left_name)
            right.save(processed_dir / right_name)

            print(f"Processed: {img_path.name} -> {left_name}, {right_name}")
