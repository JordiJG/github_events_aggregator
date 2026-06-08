"""
Streamlit Dashboard — GitHub Events Daily Aggregates (January 2024)
====================================================================
Reads the Parquet outputs produced by spark_job.py and renders four
interactive panels:

  1. Summary KPI metrics
  2. Top repositories by stars (bar chart)
  3. Top repositories by forks / issues / PRs (tabbed bar charts)
  4. Top users by total activity (table)
  5. Daily event-count trend across the month (line chart)

Prerequisites:
    pip install streamlit pandas pyarrow

Run:
    streamlit run dashboard/app.py

The dashboard expects Parquet output at:
    data/output/parquet/repo_aggregates/
    data/output/parquet/user_aggregates/
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

# ── Paths ─────────────────────────────────────────────────────────────────────
_HERE        = Path(__file__).resolve().parent
PROJECT_ROOT = _HERE.parent
PARQUET_DIR  = PROJECT_ROOT / "data" / "output" / "parquet"
REPO_PATH    = PARQUET_DIR / "repo_aggregates"
USER_PATH    = PARQUET_DIR / "user_aggregates"


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_repo() -> pd.DataFrame:
    return pd.read_parquet(REPO_PATH)


@st.cache_data
def load_user() -> pd.DataFrame:
    return pd.read_parquet(USER_PATH)


# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="GitHub Events — January 2024",
    page_icon="📊",
    layout="wide",
)

st.title("📊 GitHub Events — Daily Aggregates")
st.caption("Data source: [GHArchive](https://www.gharchive.org/) · January 2024 · Powered by PySpark + Delta Lake")

# ── Load data ─────────────────────────────────────────────────────────────────
try:
    repo_df = load_repo()
    user_df = load_user()
except Exception as exc:
    st.error(
        f"**Could not load Parquet data** from `{PARQUET_DIR}`.\n\n"
        "Run the Spark job first:\n"
        "```bash\npython -m github_aggregator.spark_job\n```"
    )
    st.exception(exc)
    st.stop()

repo_df["date"] = pd.to_datetime(repo_df["date"])
user_df["date"] = pd.to_datetime(user_df["date"])

# ── Sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Filters")

date_min = repo_df["date"].min().date()
date_max = repo_df["date"].max().date()
date_range = st.sidebar.date_input(
    "Date range",
    value=(date_min, date_max),
    min_value=date_min,
    max_value=date_max,
)

top_n = st.sidebar.slider("Top N", min_value=5, max_value=50, value=20, step=5)

if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
else:
    start, end = pd.Timestamp(date_min), pd.Timestamp(date_max)

repo_f = repo_df[(repo_df["date"] >= start) & (repo_df["date"] <= end)]
user_f = user_df[(user_df["date"] >= start) & (user_df["date"] <= end)]

# ── 1. Summary KPIs ───────────────────────────────────────────────────────────
st.subheader("📈 Summary")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Stars",      f"{int(repo_f['num_stars'].sum()):,}")
c2.metric("Total Forks",      f"{int(repo_f['num_forks'].sum()):,}")
c3.metric("Issues Created",   f"{int(repo_f['num_issues_created'].sum()):,}")
c4.metric("PRs Created",      f"{int(repo_f['num_prs_created'].sum()):,}")

st.divider()

# ── 2. Top repos by stars ─────────────────────────────────────────────────────
st.subheader("⭐ Top Repositories by Stars")
top_stars = (
    repo_f.groupby(["repo_id", "repo_name"])["num_stars"]
    .sum()
    .reset_index()
    .nlargest(top_n, "num_stars")
    .set_index("repo_name")["num_stars"]
)
st.bar_chart(top_stars)

st.divider()

# ── 3. Top repos — other metrics ──────────────────────────────────────────────
st.subheader("🔍 Top Repositories — Other Metrics")
tab_forks, tab_issues, tab_prs = st.tabs(["🍴 Forks", "🐛 Issues Created", "🔀 PRs Created"])

with tab_forks:
    data = (
        repo_f.groupby(["repo_id", "repo_name"])["num_forks"]
        .sum().reset_index().nlargest(top_n, "num_forks")
        .set_index("repo_name")["num_forks"]
    )
    st.bar_chart(data)

with tab_issues:
    data = (
        repo_f.groupby(["repo_id", "repo_name"])["num_issues_created"]
        .sum().reset_index().nlargest(top_n, "num_issues_created")
        .set_index("repo_name")["num_issues_created"]
    )
    st.bar_chart(data)

with tab_prs:
    data = (
        repo_f.groupby(["repo_id", "repo_name"])["num_prs_created"]
        .sum().reset_index().nlargest(top_n, "num_prs_created")
        .set_index("repo_name")["num_prs_created"]
    )
    st.bar_chart(data)

st.divider()

# ── 4. Top users ──────────────────────────────────────────────────────────────
st.subheader("👤 Top Users by Activity")
user_agg = (
    user_f
    .groupby(["user_id", "user_login"])[
        ["num_starred_projects", "num_issues_created", "num_prs_created"]
    ]
    .sum()
    .reset_index()
)
user_agg["total_activity"] = (
    user_agg["num_starred_projects"]
    + user_agg["num_issues_created"]
    + user_agg["num_prs_created"]
)
top_users = (
    user_agg.nlargest(top_n, "total_activity")
    [["user_login", "num_starred_projects", "num_issues_created", "num_prs_created", "total_activity"]]
    .rename(columns={
        "user_login":            "User",
        "num_starred_projects":  "Stars Given",
        "num_issues_created":    "Issues Opened",
        "num_prs_created":       "PRs Opened",
        "total_activity":        "Total",
    })
    .reset_index(drop=True)
)
st.dataframe(top_users, use_container_width=True)

st.divider()

# ── 5. Daily trend ────────────────────────────────────────────────────────────
st.subheader("📅 Daily Event Trend")
daily = (
    repo_f
    .groupby("date")[["num_stars", "num_forks", "num_issues_created", "num_prs_created"]]
    .sum()
    .sort_index()
    .rename(columns={
        "num_stars":           "Stars",
        "num_forks":           "Forks",
        "num_issues_created":  "Issues Created",
        "num_prs_created":     "PRs Created",
    })
)
st.line_chart(daily)
