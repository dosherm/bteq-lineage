#!/usr/bin/env python3
"""
dedup_folder.py — delete duplicate files based on content, not filename.

Usage:
    python3 dedup_folder.py <folder>          # dry run — shows what would be deleted
    python3 dedup_folder.py <folder> --delete # actually delete the duplicates
"""

import sys
import hashlib
from pathlib import Path


def md5(path: Path) -> str:
    h = hashlib.md5()
    h.update(path.read_bytes())
    return h.hexdigest()


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 dedup_folder.py <folder> [--delete]")
        sys.exit(1)

    folder = Path(sys.argv[1])
    do_delete = "--delete" in sys.argv

    if not folder.is_dir():
        print(f"Error: {folder} is not a folder")
        sys.exit(1)

    # Group all files by their MD5 hash
    by_hash: dict[str, list[Path]] = {}
    all_files = sorted(folder.iterdir())
    for f in all_files:
        if f.is_file():
            h = md5(f)
            by_hash.setdefault(h, []).append(f)

    # Find groups with more than one file
    duplicates = {h: paths for h, paths in by_hash.items() if len(paths) > 1}

    if not duplicates:
        print("No duplicate files found.")
        return

    total_to_delete = 0
    for h, paths in duplicates.items():
        keeper = paths[0]
        to_delete = paths[1:]
        print(f"\nKeep:   {keeper.name}")
        for p in to_delete:
            print(f"Delete: {p.name}")
            total_to_delete += 1

    print(f"\n{'─'*60}")
    print(f"  {len(duplicates)} duplicate groups — {total_to_delete} files to delete")

    if not do_delete:
        print("\n  DRY RUN — no files deleted.")
        print("  Re-run with --delete to actually remove them:")
        print(f"  python3 dedup_folder.py {folder} --delete")
        return

    # Actually delete
    deleted = 0
    for h, paths in duplicates.items():
        for p in paths[1:]:
            p.unlink()
            deleted += 1

    print(f"\n  Deleted {deleted} files.")


if __name__ == "__main__":
    main()
