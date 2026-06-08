"""
Shared pytest fixtures
======================
SparkSession:     session-scoped — created once, reused across the entire
                  test run to avoid the 10–20 s JVM startup overhead.

sample_events_df: a synthetic parsed-events DataFrame (output schema of
                  parse_events) covering all 4 relevant event types, two
                  dates, two repos, and three users — enough to exercise
                  every metric in both aggregate functions.
"""

from __future__ import annotations

import datetime
import os
import sys

import pytest
from pyspark.sql import Row, SparkSession

# On Windows, PySpark workers default to "python3" which does not exist.
# Point them at the same interpreter that is running the tests.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ── SparkSession ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Local SparkSession reused for the full test session."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("GitHubAggregatorTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


# ── Parsed-events schema ──────────────────────────────────────────────────────

PARSED_SCHEMA = StructType([
    StructField("event_type",  StringType(), True),
    StructField("user_id",     LongType(),   True),
    StructField("user_login",  StringType(), True),
    StructField("repo_id",     LongType(),   True),
    StructField("repo_name",   StringType(), True),
    StructField("date",        DateType(),   True),
    StructField("action",      StringType(), True),
])

# ── Synthetic sample events ───────────────────────────────────────────────────

DATE_A = datetime.date(2024, 1, 1)
DATE_B = datetime.date(2024, 1, 2)

# fmt: off
SAMPLE_ROWS = [
    # ── DATE_A · Repo 1 (octocat/Hello-World, id=100) ─────────────────────────
    # 2 distinct users starred  → num_stars = 2
    Row("WatchEvent",       1, "alice", 100, "octocat/Hello-World", DATE_A, "started"),
    Row("WatchEvent",       2, "bob",   100, "octocat/Hello-World", DATE_A, "started"),
    # 1 fork
    Row("ForkEvent",        1, "alice", 100, "octocat/Hello-World", DATE_A, None),
    # 1 issue opened, 1 closed (only "opened" counts)
    Row("IssuesEvent",      1, "alice", 100, "octocat/Hello-World", DATE_A, "opened"),
    Row("IssuesEvent",      2, "bob",   100, "octocat/Hello-World", DATE_A, "closed"),
    # 1 PR opened, 1 closed
    Row("PullRequestEvent", 2, "bob",   100, "octocat/Hello-World", DATE_A, "opened"),
    Row("PullRequestEvent", 1, "alice", 100, "octocat/Hello-World", DATE_A, "closed"),

    # ── DATE_A · Repo 2 (torvalds/linux, id=200) ──────────────────────────────
    # 1 star (carol), 0 forks, 2 issues opened, 0 PRs
    Row("WatchEvent",       3, "carol", 200, "torvalds/linux",       DATE_A, "started"),
    Row("IssuesEvent",      2, "bob",   200, "torvalds/linux",       DATE_A, "opened"),
    Row("IssuesEvent",      3, "carol", 200, "torvalds/linux",       DATE_A, "opened"),

    # ── DATE_B · Repo 1 ───────────────────────────────────────────────────────
    # alice starred twice → countDistinct still = 1
    Row("WatchEvent",       1, "alice", 100, "octocat/Hello-World", DATE_B, "started"),
    Row("WatchEvent",       1, "alice", 100, "octocat/Hello-World", DATE_B, "started"),
    # 1 PR opened (carol)
    Row("PullRequestEvent", 3, "carol", 100, "octocat/Hello-World", DATE_B, "opened"),

    # ── DATE_B · Repo 2 ───────────────────────────────────────────────────────
    # 2 distinct forks, 1 PR opened
    Row("ForkEvent",        2, "bob",   200, "torvalds/linux",       DATE_B, None),
    Row("ForkEvent",        3, "carol", 200, "torvalds/linux",       DATE_B, None),
    Row("PullRequestEvent", 1, "alice", 200, "torvalds/linux",       DATE_B, "opened"),

    # ── Unrelated event types (must NOT affect any metric) ────────────────────
    Row("PushEvent",        1, "alice", 100, "octocat/Hello-World", DATE_A, None),
    Row("CreateEvent",      2, "bob",   200, "torvalds/linux",       DATE_A, None),
    Row("DeleteEvent",      3, "carol", 100, "octocat/Hello-World", DATE_B, None),
    Row("ReleaseEvent",     1, "alice", 200, "torvalds/linux",       DATE_B, None),
]
# fmt: on


@pytest.fixture(scope="session")
def sample_events_df(spark: SparkSession):
    """Parsed-events DataFrame with 20 synthetic rows (session-scoped)."""
    return spark.createDataFrame(SAMPLE_ROWS, schema=PARSED_SCHEMA)
