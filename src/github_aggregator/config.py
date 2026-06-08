"""Central configuration — paths, default month, Spark settings."""

from pathlib import Path

# Root of the project: src/github_aggregator/ → src/ → project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# ── Data directories ──────────────────────────────────────────────────────────
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_DIR   = PROJECT_ROOT / "data" / "output"

OUTPUT_CSV_DIR     = OUTPUT_DIR / "csv"
OUTPUT_PARQUET_DIR = OUTPUT_DIR / "parquet"
OUTPUT_DELTA_DIR   = OUTPUT_DIR / "delta"

# ── GHArchive settings ────────────────────────────────────────────────────────
GHARCHIVE_BASE_URL = "https://data.gharchive.org"
DEFAULT_MONTH      = "2024-01"

# ── Spark settings ────────────────────────────────────────────────────────────
SPARK_APP_NAME      = "GitHubEventsAggregator"
DELTA_SQL_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG_CLASS = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
