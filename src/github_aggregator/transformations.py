"""
Spark transformations
=====================
Pure functions — no I/O, no SparkSession creation — for maximum testability.

Event → metric mapping
──────────────────────
  WatchEvent                          → star   (actor starred a repo)
  ForkEvent                           → fork   (actor forked a repo)
  IssuesEvent       + action=opened   → issue created
  PullRequestEvent  + action=opened   → PR created
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

# Explicit schema for raw GHArchive events.
# Avoids full-scan schema inference and handles `type` (Spark 4 reserved keyword).
# Shared by the Spark job and test fixtures to avoid duplication.
RAW_SCHEMA = StructType([
    StructField("type", StringType(), True),
    StructField("actor", StructType([
        StructField("id",    LongType(),   True),
        StructField("login", StringType(), True),
    ]), True),
    StructField("repo", StructType([
        StructField("id",   LongType(),   True),
        StructField("name", StringType(), True),
    ]), True),
    StructField("payload", StructType([
        StructField("action", StringType(), True),
    ]), True),
    StructField("created_at", StringType(), True),
])


def parse_events(df: DataFrame) -> DataFrame:
    """Flatten raw GHArchive records into a clean, analysis-ready DataFrame.

    Expected input columns (produced by ``spark.read.json``):
        type, actor.id, actor.login, repo.id, repo.name,
        created_at, payload.action

    Output columns:
        event_type  STRING
        user_id     LONG
        user_login  STRING
        repo_id     LONG
        repo_name   STRING
        date        DATE
        action      STRING
    """
    return (
        df.select(
            # `type` requires backtick-quoting in Spark 4 (reserved keyword)
            F.col("`type`").alias("event_type"),
            F.col("actor.id").cast(LongType()).alias("user_id"),
            F.col("actor.login").alias("user_login"),
            F.col("repo.id").cast(LongType()).alias("repo_id"),
            F.col("repo.name").alias("repo_name"),
            F.to_date(F.col("created_at")).alias("date"),
            F.col("payload.action").alias("action"),
        )
        .filter(F.col("event_type").isNotNull())
        .filter(F.col("date").isNotNull())
    )


def compute_repo_aggregates(df: DataFrame) -> DataFrame:
    """Compute per-repository daily aggregates.

    Input:  parsed events DataFrame (output of ``parse_events``).

    Output schema:
        date                DATE
        repo_id             LONG
        repo_name           STRING
        num_stars           LONG  — distinct users who starred the repo
        num_forks           LONG  — distinct users who forked the repo
        num_issues_created  LONG  — IssuesEvent where action = 'opened'
        num_prs_created     LONG  — PullRequestEvent where action = 'opened'
    """
    return (
        df.groupBy("date", "repo_id", "repo_name")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "WatchEvent", F.col("user_id"))
            ).alias("num_stars"),
            F.countDistinct(
                F.when(F.col("event_type") == "ForkEvent", F.col("user_id"))
            ).alias("num_forks"),
            F.count(
                F.when(
                    (F.col("event_type") == "IssuesEvent") & (F.col("action") == "opened"),
                    F.lit(1),
                )
            ).alias("num_issues_created"),
            F.count(
                F.when(
                    (F.col("event_type") == "PullRequestEvent") & (F.col("action") == "opened"),
                    F.lit(1),
                )
            ).alias("num_prs_created"),
        )
    )


def compute_user_aggregates(df: DataFrame) -> DataFrame:
    """Compute per-user daily aggregates.

    Input:  parsed events DataFrame (output of ``parse_events``).

    Output schema:
        date                  DATE
        user_id               LONG
        user_login            STRING
        num_starred_projects  LONG  — repos the user starred
        num_issues_created    LONG  — issues the user opened
        num_prs_created       LONG  — PRs the user opened
    """
    return (
        df.groupBy("date", "user_id", "user_login")
        .agg(
            F.count(
                F.when(F.col("event_type") == "WatchEvent", F.lit(1))
            ).alias("num_starred_projects"),
            F.count(
                F.when(
                    (F.col("event_type") == "IssuesEvent") & (F.col("action") == "opened"),
                    F.lit(1),
                )
            ).alias("num_issues_created"),
            F.count(
                F.when(
                    (F.col("event_type") == "PullRequestEvent") & (F.col("action") == "opened"),
                    F.lit(1),
                )
            ).alias("num_prs_created"),
        )
    )
