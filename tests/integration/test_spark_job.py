"""
Integration tests for spark_job.py  (primordial / fast path)
=============================================================
Strategy
--------
* Reuse the session-scoped ``spark`` SparkSession from conftest.py
  (already started by unit tests — zero extra JVM startup cost).
* Call parse_events + compute_*_aggregates directly — no disk writes,
  no winutils.exe, no Maven downloads.
* One extra test reads a real .json.gz to exercise the production read path.

Run all tests together for best performance (one JVM for everything):

    pytest -v
"""

from __future__ import annotations

import gzip
import json

import pytest
from pyspark.sql import Row, functions as F

from github_aggregator.transformations import (
    RAW_SCHEMA,
    compute_repo_aggregates,
    compute_user_aggregates,
    parse_events,
)

# ── Synthetic events ──────────────────────────────────────────────────────────

_EVENTS = [
    Row(type="WatchEvent",       actor=Row(id=1, login="alice"), repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action="started"), created_at="2024-01-05T10:00:00Z"),
    Row(type="WatchEvent",       actor=Row(id=2, login="bob"),   repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action="started"), created_at="2024-01-05T11:00:00Z"),
    Row(type="ForkEvent",        actor=Row(id=3, login="carol"), repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action=None),      created_at="2024-01-05T12:00:00Z"),
    Row(type="IssuesEvent",      actor=Row(id=1, login="alice"), repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action="opened"),  created_at="2024-01-05T13:00:00Z"),
    Row(type="IssuesEvent",      actor=Row(id=2, login="bob"),   repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action="closed"),  created_at="2024-01-05T13:30:00Z"),
    Row(type="PullRequestEvent", actor=Row(id=2, login="bob"),   repo=Row(id=200, name="torvalds/linux"),      payload=Row(action="opened"),  created_at="2024-01-05T14:00:00Z"),
    Row(type="IssuesEvent",      actor=Row(id=3, login="carol"), repo=Row(id=200, name="torvalds/linux"),      payload=Row(action="closed"),  created_at="2024-01-05T15:00:00Z"),
    Row(type="PushEvent",        actor=Row(id=1, login="alice"), repo=Row(id=100, name="octocat/Hello-World"), payload=Row(action=None),      created_at="2024-01-05T16:00:00Z"),
]

# ── Fixtures (module-scoped, reuse the session SparkSession) ─────────────────

@pytest.fixture(scope="module")
def repo_agg(spark):
    raw = spark.createDataFrame(_EVENTS, schema=RAW_SCHEMA)
    return compute_repo_aggregates(parse_events(raw)).cache()


@pytest.fixture(scope="module")
def user_agg(spark):
    raw = spark.createDataFrame(_EVENTS, schema=RAW_SCHEMA)
    return compute_user_aggregates(parse_events(raw)).cache()


# ── Output schemas ────────────────────────────────────────────────────────────

class TestPipelineSchemas:
    def test_repo_schema(self, repo_agg):
        """End-to-end pipeline produces all 7 repo fields required by the assessment."""
        assert set(repo_agg.columns) == {
            "date", "repo_id", "repo_name",
            "num_stars", "num_forks", "num_issues_created", "num_prs_created",
        }

    def test_user_schema(self, user_agg):
        """End-to-end pipeline produces all 6 user fields required by the assessment."""
        assert set(user_agg.columns) == {
            "date", "user_id", "user_login",
            "num_starred_projects", "num_issues_created", "num_prs_created",
        }


# ── Key business rules ────────────────────────────────────────────────────────

class TestPipelineBusinessRules:
    def test_opened_only_rule_and_unrelated_events_ignored(self, repo_agg, user_agg):
        """Core business rule: only 'opened' actions count; closed events and
        unrelated event types (PushEvent) must not inflate any metric."""
        # repo_id=100: 1 issue opened + 1 closed → 1; PushEvent → 0 forks added
        repo_row = repo_agg.filter(F.col("repo_id") == 100).collect()[0]
        assert repo_row["num_issues_created"] == 1
        assert repo_row["num_forks"] == 1           # ForkEvent only; PushEvent ignored

        # user_id=1 (alice): 1 star + 1 issue; PushEvent must not add anything
        user_row = user_agg.filter(F.col("user_id") == 1).collect()[0]
        total = user_row["num_starred_projects"] + user_row["num_issues_created"] + user_row["num_prs_created"]
        assert total == 2   # 1 star + 1 issue opened; PushEvent contributes 0


# ── .json.gz read path ────────────────────────────────────────────────────────

class TestGzipReadPath:
    """Exercises the actual production read path: spark.read.schema().json(gz_file)."""

    def test_end_to_end_from_gz_file(self, spark, tmp_path):
        gz_file = tmp_path / "2024-01-05-0.json.gz"
        events = [
            {"type": "WatchEvent",  "actor": {"id": 1, "login": "alice"},
             "repo": {"id": 10, "name": "a/b"}, "payload": {"action": "started"},
             "created_at": "2024-01-05T00:00:00Z"},
            {"type": "ForkEvent",   "actor": {"id": 2, "login": "bob"},
             "repo": {"id": 10, "name": "a/b"}, "payload": {},
             "created_at": "2024-01-05T01:00:00Z"},
            {"type": "PushEvent",   "actor": {"id": 3, "login": "carol"},
             "repo": {"id": 10, "name": "a/b"}, "payload": {},
             "created_at": "2024-01-05T02:00:00Z"},
        ]
        with gzip.open(gz_file, "wt", encoding="utf-8") as fh:
            for e in events:
                fh.write(json.dumps(e) + "\n")

        raw    = spark.read.schema(RAW_SCHEMA).json(str(gz_file))
        parsed = parse_events(raw)
        repo   = compute_repo_aggregates(parsed)

        assert parsed.count() == 3
        row = repo.collect()[0]
        assert row["num_stars"] == 1
        assert row["num_forks"] == 1
