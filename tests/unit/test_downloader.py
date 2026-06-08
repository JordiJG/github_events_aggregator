"""
Unit tests for downloader.py
==============================
No network calls are made -- requests.get is mocked where needed.

Coverage:
  generate_urls  - correct total count for the assessment month; URL format
  download_file  - successful write to disk; HTTP error handled gracefully
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import requests

from github_aggregator.downloader import download_file, generate_urls


# -- generate_urls -------------------------------------------------------------

class TestGenerateUrls:
    def test_january_2024_produces_744_urls(self):
        """31 days x 24 hours = 744 files -- covers the full assessment month."""
        assert len(generate_urls("2024-01")) == 744

    def test_url_format(self):
        """First URL is 2024-01-01-0.json.gz (hour not zero-padded, day zero-padded)."""
        assert generate_urls("2024-01")[0] == "https://data.gharchive.org/2024-01-01-0.json.gz"


# -- download_file -------------------------------------------------------------

class TestDownloadFile:
    def test_downloads_and_writes_file(self, tmp_path):
        """A successful response body is written to disk and the Path is returned."""
        fake_content = b"fake gzip content"
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.iter_content.return_value = [fake_content]

        with patch("github_aggregator.downloader.requests.get", return_value=mock_resp):
            result = download_file(
                "https://data.gharchive.org/2024-01-02-5.json.gz", tmp_path
            )

        assert result == tmp_path / "2024-01-02-5.json.gz"
        assert result.read_bytes() == fake_content

    def test_returns_none_on_http_error(self, tmp_path):
        """HTTP errors are caught; None is returned and no partial file is left on disk."""
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

        with patch("github_aggregator.downloader.requests.get", return_value=mock_resp):
            result = download_file(
                "https://data.gharchive.org/2024-01-03-10.json.gz", tmp_path
            )

        assert result is None
        assert not (tmp_path / "2024-01-03-10.json.gz").exists()
