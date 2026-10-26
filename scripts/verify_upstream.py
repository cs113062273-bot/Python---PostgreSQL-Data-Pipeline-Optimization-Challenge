"""Verify every organizer file against the pinned content manifest."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def verify():
    manifest = json.loads((ROOT / "upstream.json").read_text(encoding="utf-8"))
    for name, expected in manifest["files_sha256"].items():
        actual = hashlib.sha256((ROOT / "app" / name).read_bytes()).hexdigest()
        if actual != expected:
            raise AssertionError(f"Organizer file changed: {name}")
    print(f"{len(manifest['files_sha256'])} organizer files unchanged; commit {manifest['official_commit']}")


if __name__ == "__main__":
    verify()
