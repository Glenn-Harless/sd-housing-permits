"""Download development permit CSVs from seshat.datasd.org."""

from __future__ import annotations

from pathlib import Path

import httpx

RAW_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

SOURCES: dict[str, str] = {
    "set1_active": "https://seshat.datasd.org/development_permits_set1/permits_set1_active_datasd.csv",
    "set1_closed": "https://seshat.datasd.org/development_permits_set1/permits_set1_closed_datasd.csv",
    "set2_active": "https://seshat.datasd.org/development_permits_set2/permits_set2_active_datasd.csv",
    "set2_closed": "https://seshat.datasd.org/development_permits_set2/permits_set2_closed_datasd.csv",
    "tags": "https://seshat.datasd.org/development_permits_tags/permits_project_tags_datasd.csv",
}


def download(name: str, url: str, *, force: bool = False) -> Path:
    """Download a single CSV. Skips if file exists and force=False."""
    dest = RAW_DIR / f"{name}.csv"
    if dest.exists() and not force:
        print(f"  [skip] {name} (already exists, {dest.stat().st_size:,} bytes)")
        return dest

    print(f"  [download] {name} ...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  [done] {name} -> {dest.stat().st_size:,} bytes")
    return dest


def ingest(*, force: bool = False) -> list[Path]:
    """Download all source CSVs. Returns list of downloaded file paths."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, url in SOURCES.items():
        try:
            paths.append(download(name, url, force=force))
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                print(f"  [warn] {name}: 403 forbidden, skipping")
            else:
                raise
    return paths


if __name__ == "__main__":
    ingest()
