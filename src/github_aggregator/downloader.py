"""
GHArchive downloader
====================
Downloads hourly GitHub event files for a given calendar month.

URL pattern:  https://data.gharchive.org/YYYY-MM-DD-H.json.gz
  - DD : zero-padded day  (01–31)
  - H  : non-zero-padded hour (0–23)

Usage (CLI):
    python -m github_aggregator.downloader
    python -m github_aggregator.downloader --month 2024-01 --output data/raw --workers 8

Installed entry-point:
    download-gharchive --month 2024-01
"""

from __future__ import annotations

import argparse
import calendar
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from github_aggregator.config import DEFAULT_MONTH, GHARCHIVE_BASE_URL, RAW_DATA_DIR

logger = logging.getLogger(__name__)


def generate_urls(month: str) -> list[str]:
    """Return all hourly GHArchive URLs for *month* (format: YYYY-MM).

    January 2024 → 31 days × 24 hours = 744 URLs.
    The hour component is NOT zero-padded (GHArchive convention).
    """
    year_str, month_str = month.split("-")
    year, month_num = int(year_str), int(month_str)
    days_in_month = calendar.monthrange(year, month_num)[1]

    urls: list[str] = []
    for day in range(1, days_in_month + 1):
        for hour in range(24):
            filename = f"{year_str}-{month_str}-{day:02d}-{hour}.json.gz"
            urls.append(f"{GHARCHIVE_BASE_URL}/{filename}")
    return urls


def download_file(url: str, output_dir: Path) -> Path | None:
    """Download *url* into *output_dir*.

    Skips files that already exist (resume-safe).
    Returns the local Path on success, None on failure.
    """
    filename = url.split("/")[-1]
    dest = output_dir / filename

    if dest.exists():
        logger.debug("Skipping (already exists): %s", filename)
        return dest

    try:
        response = requests.get(url, timeout=60, stream=True)
        response.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in response.iter_content(chunk_size=1 << 16):  # 64 KB chunks
                fh.write(chunk)
        return dest
    except requests.RequestException as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        dest.unlink(missing_ok=True)  # remove any partial file so resume-safe check stays valid
        return None


def download_month(
    month: str = DEFAULT_MONTH,
    output_dir: Path = RAW_DATA_DIR,
    max_workers: int = 8,
) -> list[Path]:
    """Download all hourly files for *month* into *output_dir*.

    Uses a thread pool for concurrent downloads.
    Returns a list of successfully downloaded (or already-present) local Paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    urls = generate_urls(month)

    logger.info(
        "Downloading %d files for month %s into %s",
        len(urls), month, output_dir,
    )

    results: list[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(download_file, url, output_dir): url for url in urls}
        with tqdm(total=len(urls), desc=f"Downloading {month}", unit="file") as progress:
            for future in as_completed(futures):
                path = future.result()
                if path is not None:
                    results.append(path)
                progress.update(1)

    logger.info(
        "Download complete: %d / %d files available.",
        len(results), len(urls),
    )
    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Download GHArchive hourly event files for a calendar month.",
    )
    parser.add_argument(
        "--month",
        default=DEFAULT_MONTH,
        help="Month to download in YYYY-MM format (default: %(default)s)",
    )
    parser.add_argument(
        "--output",
        default=str(RAW_DATA_DIR),
        help="Destination directory for downloaded files (default: %(default)s)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Number of parallel download threads (default: %(default)s)",
    )
    args = parser.parse_args()

    download_month(
        month=args.month,
        output_dir=Path(args.output),
        max_workers=args.workers,
    )


if __name__ == "__main__":
    main()
