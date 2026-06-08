"""
scripts/setup_winutils.py
=========================
Sets up Hadoop winutils for PySpark on Windows.

PySpark requires ``winutils.exe`` to write files to the local Windows
filesystem.  This script downloads a pre-built winutils binary from the
official GitHub release and configures HADOOP_HOME so that Spark can find it.

Usage:
    py scripts/setup_winutils.py

After running:
  - Restart your terminal so HADOOP_HOME is in your environment
  - OR add it to your user environment variables permanently

Requirements:
    pip install requests

Note:
    This script is only needed on Windows. On Linux/macOS, PySpark writes
    to the local filesystem without any extra setup.
"""

from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

# ── Configuration ─────────────────────────────────────────────────────────────
HADOOP_VERSION = "3.4.1"
_BASE_URL = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{HADOOP_VERSION}/bin"
# Both winutils.exe AND hadoop.dll are required; NativeIO$Windows fails without hadoop.dll
_BINARIES = ["winutils.exe", "hadoop.dll"]
INSTALL_DIR = Path.home() / "hadoop" / f"hadoop-{HADOOP_VERSION}"
WINUTILS_PATH = INSTALL_DIR / "bin" / "winutils.exe"


def main() -> None:
    if sys.platform != "win32":
        print("This script is only needed on Windows. Nothing to do.")
        return

    if WINUTILS_PATH.is_file() and (INSTALL_DIR / "bin" / "hadoop.dll").is_file():
        print(f"winutils.exe and hadoop.dll already exist at: {INSTALL_DIR / 'bin'}")
        _print_env_instructions()
        return

    print(f"Downloading Hadoop {HADOOP_VERSION} binaries (winutils.exe + hadoop.dll) ...")
    WINUTILS_PATH.parent.mkdir(parents=True, exist_ok=True)

    for binary in _BINARIES:
        dest = INSTALL_DIR / "bin" / binary
        if dest.is_file():
            print(f"  Already present: {binary}")
            continue
        url = f"{_BASE_URL}/{binary}"
        try:
            urllib.request.urlretrieve(url, dest)
            print(f"  Downloaded: {dest}")
        except Exception as exc:
            print(f"\nERROR: Failed to download {binary}: {exc}")
            print(
                f"\nManual download:\n"
                f"  1. Go to: {url}\n"
                f"  2. Save the file to: {dest}\n"
                f"  3. Set HADOOP_HOME = {INSTALL_DIR}\n"
            )
            sys.exit(1)

    _print_env_instructions()


def _print_env_instructions() -> None:
    install_dir = INSTALL_DIR

    print(
        f"\n{'='*60}\n"
        f"Set HADOOP_HOME and restart your terminal:\n\n"
        f"  PowerShell (current session only):\n"
        f"    $env:HADOOP_HOME = '{install_dir}'\n\n"
        f"  PowerShell (permanent, current user):\n"
        f"    [Environment]::SetEnvironmentVariable('HADOOP_HOME', '{install_dir}', 'User')\n\n"
        f"  Then run the integration tests:\n"
        f"    pytest tests/integration/ -v\n"
        f"{'='*60}\n"
    )


if __name__ == "__main__":
    main()
