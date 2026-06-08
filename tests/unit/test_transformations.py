"""
Unit tests for transformations.py
===================================
All tests use the session-scoped ``spark`` and ``sample_events_df``
fixtures from conftest.py.  No I/O -- pure Spark DataFrame operations.

Coverage:
  parse_events            - output schema contract; malformed rows filtered
  compute_repo_aggregates - required output fields; stars distinct-count; opened-only rule
  compute_user_aggregates - required output fields; cross-repo aggregation; opened-only rule;
                            starred-projects counts distinct repos (not raw WatchEvent rows)
"""

from __future__ import annotations

import datetime

from pyspark.sql import Row, functions as F

from github_aggregator.transformations import (
    RAW_SCHEMA,
    compute_repo_aggregates,
    compute_user_aggregates,
    parse_events,
)


# -- Helpers -------------------------------------------------------------------

def _raw(spark, rows):
    return spark.createDataFrame(rows, schema=RAW_SCHEMA)


def _repo_row(df, date: datetime.date, repo_id: int):
    rows = df.filter((F.col("date") == date) & (F.col("repo_id") == repo_id)).collect()
    assert rows, f"No repo row for date={date}, repo_id={repo_id}"
    return rows[0]


def _user_row(df, date: datetime.date, user_id: int):
    rows = df.filter((F.col("date") == date) & (F.col("user_id") == user_id)).collect()
    assert rows, f"No user row for date={date}, user_id={user_id}"
    return rows[0]


# -- parse_events --------------------------------------------------------------

class TestParseEvents:
    def test_output_columns(self, spark):
        """parse_events produces exactly the 7 columns required downstream."""
        raw = _raw(spark, [
            Row(type="WatchEvent", actor=Row(id=1, login="alice"),
                repo=Row(id=100, name="octocat/Hello-World"),
                payload=Row(action="started"), created_at="2024-01-01T00:00:00Z"),
        ])
        assert set(parse_events(raw).columns) == {
            "event_type", "user_id", "user_login", "repo_id", "repo_name", "date", "action"
        }

    def test_filters_malformed_rows(self, spark):
        """Rows with null event_type (malformed events) are silently dropped."""
        raw = _raw(spark, [
            Row(type="WatchEvent", actor=Row(id=1, login="alice"),
                repo=Row(id=100, name="r/r"), payload=Row(action="started"),
                created_at="2024-01-01T00:00:00Z"),
            Row(type=None, actor=Row(id=2, login="bob"),
                repo=Row(id=200, name="r/s"), payload=Row(action=None),
                created_at="2024-01-01T01:00:00Z"),
        ])
        assert parse_events(raw).count() == 1


# -- compute_repo_aggregates ---------------------------------------------------

class TestComputeRepoAggregates:
    def test_output_schema(self, sample_events_df):
        """Output contains exactly the 7 fields required by the assessment."""
        assert set(compute_repo_aggregates(sample_events_df).columns) == {
            "date", "repo_id", "repo_name",
            "num_stars", "num_forks", "num_issues_created", "num_prs_created",
        }

    def test_stars_count_distinct_users(self, sample_events_df):
        """DATE_A, Repo 1: alice + bob each starred once -> 2 distinct stars."""
        row = _repo_row(compute_repo_aggregates(sample_events_df), datetime.date(2024, 1, 1), 100)
        assert row["num_stars"] == 2

    def test_issues_and_prs_count_only_opened_action(self, sample_events_df):
        """DATE_A, Repo 1: 1 issue opened + 1 closed -> 1 counted; same for PRs."""
        row = _repo_row(compute_repo_aggregates(sample_events_df), datetime.date(2024, 1, 1), 100)
        assert row["num_issues_created"] == 1
        assert row["num_prs_created"] == 1


# -- compute_user_aggregates ---------------------------------------------------

class TestComputeUserAggregates:
    def test_output_schema(self, sample_events_df):
        """Output contains exactly the 6 fields required by the assessment."""
        assert set(compute_user_aggregates(sample_events_df).columns) == {
            "date", "user_id", "user_login",
            "num_starred_projects", "num_issues_created", "num_prs_created",
        }

    def test_user_aggregated_across_repos(self, sample_events_df):
        """DATE_A: bob starred Repo 1 and opened an issue on Repo 2 -- both counted."""
        row = _user_row(compute_user_aggregates(sample_events_df), datetime.date(2024, 1, 1), 2)
        assert row["num_starred_projects"] == 1
        assert row["num_issues_created"] == 1

    def test_closed_events_not_counted(self, sample_events_df):
        """DATE_A: alice closed 1 PR -- num_prs_created must remain 0."""
        row = _user_row(compute_user_aggregates(sample_events_df), datetime.date(2024, 1, 1), 1)
        assert row["num_prs_created"] == 0

    def test_starred_projects_counts_distinct_repos(self, sample_events_df):
        """DATE_B: alice fires 2 WatchEvents on the same repo -> num_starred_projects = 1.

        Regression guard for the countDistinct(repo_id) fix: using count() instead
        would return 2, over-counting duplicate WatchEvent rows for the same repo.
        """
        row = _user_row(compute_user_aggregates(sample_events_df), datetime.date(2024, 1, 2), 1)
        assert row["num_starred_projects"] == 1
