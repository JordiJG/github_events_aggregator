# GitHub Events Aggregator

Daily aggregation of public GitHub events for **January 2024** using **PySpark**.  
Data source: [GHArchive](https://www.gharchive.org/) — public GitHub event archive.

## What it does

Downloads 744 hourly `.json.gz` files (31 days × 24 hours) and computes two daily aggregates:

| Aggregate | Key fields |
|---|---|
| **Repository** | date, repo id/name, stars, forks, issues created, PRs created |
| **User** | date, user id/login, starred projects, issues created, PRs created |

Each aggregate is written in three formats: **CSV**, **Parquet**, and **Delta Lake**.

---

## Project structure

```
github_events_aggregator/
├── src/github_aggregator/
│   ├── config.py            ← paths and Spark settings
│   ├── downloader.py        ← concurrent GHArchive downloader (CLI)
│   ├── transformations.py   ← pure Spark aggregate logic
│   └── spark_job.py         ← pipeline entrypoint (CLI)
├── dashboard/
│   └── app.py               ← Streamlit dashboard
├── tests/
│   ├── conftest.py          ← SparkSession + synthetic data fixtures
│   ├── unit/                ← fast tests, no Delta Lake required
│   └── integration/         ← full pipeline tests with Delta Lake
├── data/                    ← gitignored (raw downloads + output)
├── requirements.txt
└── pyproject.toml           ← installable package
```

---

## Requirements

- Python 3.9+
- **Java 17** (required by PySpark 4 — Java 11 is not supported) — verify with `java -version`
- Internet connection for the initial data download (~15–25 GB for January 2024)
- **Windows only:** `winutils.exe` + `hadoop.dll` for Hadoop 3.4.1 (see Windows Setup below)

---

## Windows Setup (Windows users only)

PySpark requires `winutils.exe` and `hadoop.dll` for Hadoop 3.4.1 (the version bundled with PySpark 4) to write files on Windows.

```bash
py scripts/setup_winutils.py
```

This downloads both binaries and prints the `HADOOP_HOME` variable to set. Set it permanently before running the pipeline:

```powershell
[Environment]::SetEnvironmentVariable('HADOOP_HOME', "$HOME\hadoop\hadoop-3.4.1", 'User')
```

Then restart your terminal.

---

## Installation

### Option A — Editable install (recommended, bonus deliverable)

```bash
git clone https://github.com/JordiJG/github_events_aggregator.git
cd github_events_aggregator
pip install -e ".[dev,dashboard]"
```

### Option B — Requirements file

```bash
pip install -r requirements.txt
```

---

## Running the pipeline

### Step 1 — Download January 2024 data

```bash
# Using the installed entry-point:
download-gharchive --month 2024-01

# Or directly:
python -m github_aggregator.downloader --month 2024-01

# Options:
#   --month   YYYY-MM   Month to download (default: 2024-01)
#   --output  PATH      Download directory (default: data/raw)
#   --workers N         Parallel threads   (default: 8)
```

Files are saved to `data/raw/` and the download is **resume-safe** — already-present files are skipped.

### Step 2 — Run the aggregation job

```bash
# Using the installed entry-point:
run-aggregator

# Or directly:
python -m github_aggregator.spark_job

# Options:
#   --raw-dir    PATH   Directory with .json.gz files (default: data/raw)
#   --output-dir PATH   Root output directory         (default: data/output)
```

Output is written to:

```
data/output/
├── csv/
│   ├── repo_aggregates/
│   └── user_aggregates/
├── parquet/
│   ├── repo_aggregates/
│   └── user_aggregates/
└── delta/
    ├── repo_aggregates/
    └── user_aggregates/
```

---

## Running the dashboard

```bash
pip install streamlit pandas pyarrow   # if not already installed
streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

> **Note:** Run the aggregation job first so Parquet output exists.

The dashboard provides:
- Summary KPIs (total stars, forks, issues, PRs)
- Top repositories by stars, forks, issues, PRs
- Top users by total activity
- Daily event trend line chart
- Interactive date range and Top-N filters

---

## Running the tests

```bash
# Unit tests only (fast, no Delta Lake JARs needed):
pytest tests/unit/ -v

# Integration tests (slower — downloads Delta JARs on first run):
pytest tests/integration/ -v

# All tests:
pytest -v

# With coverage:
pip install pytest-cov
pytest --cov=github_aggregator --cov-report=term-missing
```

### Test structure

| File | What it covers |
|---|---|
| `tests/unit/test_transformations.py` | `parse_events` schema contract and malformed-row filtering; `compute_repo_aggregates` output fields, distinct-star logic, opened-only rule; `compute_user_aggregates` output fields, cross-repo aggregation, closed-event exclusion |
| `tests/unit/test_downloader.py` | 744 URLs generated for January 2024; correct URL format; file written on success; `None` returned on HTTP error |
| `tests/integration/test_spark_job.py` | Both output schemas end-to-end; opened-only rule and unrelated-event filtering; `.json.gz` → parse → aggregate full read path |

---

## Tool justifications

| Tool | Reason |
|---|---|
| **PySpark** | Required by the assessment |
| **delta-spark** | Open-source Delta Lake — runs locally without Databricks, no extra infrastructure |
| **requests + tqdm** | Standard HTTP download with a progress bar; simpler than Spark for sequential HTTP |
| **Streamlit** | Zero-boilerplate dashboard that reads Parquet directly; demonstrates end-to-end data pipeline thinking (not required by assessment) |
| **ThreadPoolExecutor** | Concurrent downloads for 744 files; Spark is not appropriate for HTTP downloads |

---

## Data prevented from being committed

The `.gitignore` blocks all of the following:

- `data/` — raw downloads and all output files
- `*.json.gz` — compressed GHArchive event files
- `*.parquet` — Parquet output files
- `*.csv` — CSV output files
- `_delta_log/` — Delta Lake transaction logs

---

## Git workflow

```bash
# Clone the repo
git clone https://github.com/JordiJG/github_events_aggregator.git
cd github_events_aggregator

# Create an initial empty commit on main (PR target)
git commit --allow-empty -m "chore: init repository"
git push -u origin main

# Create and switch to the feature branch
git checkout -b feature/github-events-aggregator

# Add all code and push
git add .
git commit -m "feat: add GitHub events daily aggregator with dashboard"
git push -u origin feature/github-events-aggregator

# Open a Pull Request on GitHub: feature/github-events-aggregator → main
```
