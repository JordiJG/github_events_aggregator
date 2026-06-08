"""
Spark Job — GitHub Events Daily Aggregator
==========================================
Orchestrates the full pipeline:
  1. Build a SparkSession with Delta Lake support
  2. Read all raw .json.gz files from the raw data directory
  3. Parse and flatten events (parse_events)
  4. Compute repository daily aggregates
  5. Compute user daily aggregates
  6. Write each aggregate in three formats: CSV, Parquet, Delta

Usage (CLI):
    python -m github_aggregator.spark_job
    python -m github_aggregator.spark_job --raw-dir data/raw --output-dir data/output

Installed entry-point:
    run-aggregator
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# On Windows, PySpark workers default to "python3" which does not exist.
# Ensure they use the same interpreter that launched this process.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from github_aggregator.config import (
    DELTA_CATALOG_CLASS,
    DELTA_SQL_EXTENSION,
    OUTPUT_CSV_DIR,
    OUTPUT_DELTA_DIR,
    OUTPUT_PARQUET_DIR,
    RAW_DATA_DIR,
    SPARK_APP_NAME,
)
from github_aggregator.transformations import (
    RAW_SCHEMA,
    compute_repo_aggregates,
    compute_user_aggregates,
    parse_events,
)

logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    """Create (or reuse) a SparkSession configured for local + Delta Lake.

    Uses ``configure_spark_with_delta_pip`` so no Maven download is needed
    when delta-spark is already installed via pip.
    """
    builder = (
        SparkSession.builder.appName(SPARK_APP_NAME)
        .config("spark.sql.extensions", DELTA_SQL_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG_CLASS)
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _write_outputs(
    df,
    name: str,
    csv_dir: Path,
    parquet_dir: Path,
    delta_dir: Path,
) -> None:
    """Write *df* to CSV, Parquet, and Delta under the given directories."""
    csv_path = str(csv_dir / name)
    logger.info("Writing %-20s → CSV     : %s", name, csv_path)
    df.write.mode("overwrite").option("header", "true").csv(csv_path)

    parquet_path = str(parquet_dir / name)
    logger.info("Writing %-20s → Parquet : %s", name, parquet_path)
    df.write.mode("overwrite").parquet(parquet_path)

    delta_path = str(delta_dir / name)
    logger.info("Writing %-20s → Delta   : %s", name, delta_path)
    df.write.format("delta").mode("overwrite").save(delta_path)


def run(
    raw_dir: Path = RAW_DATA_DIR,
    csv_dir: Path = OUTPUT_CSV_DIR,
    parquet_dir: Path = OUTPUT_PARQUET_DIR,
    delta_dir: Path = OUTPUT_DELTA_DIR,
    spark: SparkSession | None = None,
) -> None:
    """Execute the full aggregation pipeline.

    If *spark* is provided (e.g. from a test fixture) it is reused and NOT
    stopped on completion.  When *spark* is None the function creates its own
    session and stops it when done.
    """
    _owns_session = spark is None
    if spark is None:
        spark = build_spark_session()

    for directory in (csv_dir, parquet_dir, delta_dir):
        directory.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        logger.warning(
            "Raw data directory %s does not exist — run the downloader first:\n"
            "  python -m github_aggregator.downloader",
            raw_dir,
        )
        if _owns_session:
            spark.stop()
        return

    raw_files = [f.as_posix() for f in sorted(raw_dir.glob("*.json.gz"))]
    if not raw_files:
        logger.warning(
            "No .json.gz files found in %s — run the downloader first:\n"
            "  python -m github_aggregator.downloader",
            raw_dir,
        )
        if _owns_session:
            spark.stop()
        return

    logger.info("Reading %d raw event files from %s", len(raw_files), raw_dir)
    raw_df = spark.read.schema(RAW_SCHEMA).json(raw_files)

    parsed_df = parse_events(raw_df).cache()

    logger.info("Computing repository aggregates …")
    repo_agg_df = compute_repo_aggregates(parsed_df)
    _write_outputs(repo_agg_df, "repo_aggregates", csv_dir, parquet_dir, delta_dir)

    logger.info("Computing user aggregates …")
    user_agg_df = compute_user_aggregates(parsed_df)
    _write_outputs(user_agg_df, "user_aggregates", csv_dir, parquet_dir, delta_dir)

    parsed_df.unpersist()

    if _owns_session:
        spark.stop()

    logger.info("Pipeline complete.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Run the GitHub Events daily aggregator (repo + user aggregates).",
    )
    parser.add_argument(
        "--raw-dir",
        default=str(RAW_DATA_DIR),
        help="Directory containing raw .json.gz files (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(RAW_DATA_DIR.parent / "output"),
        help="Root output directory; sub-dirs csv/, parquet/, delta/ are created automatically (default: %(default)s)",
    )
    args = parser.parse_args()

    output_root = Path(args.output_dir)
    run(
        raw_dir=Path(args.raw_dir),
        csv_dir=output_root / "csv",
        parquet_dir=output_root / "parquet",
        delta_dir=output_root / "delta",
    )


if __name__ == "__main__":
    main()
