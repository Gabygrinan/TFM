#!/usr/bin/env python3
"""
Unit tests for the elToque Historical Exporter.

Run with:
    python -m pytest tests/test_export.py -v
"""

import csv
import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add parent to path so we can import the module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from export import (
    ElToqueClient,
    extract_history,
    load_token,
    write_csv,
    parse_date,
    EARLIEST_DATE,
)


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

SAMPLE_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.fake_sig"

SAMPLE_API_RESPONSE = {
    "tasas": {
        "USD": 300.0,
        "MLC": 220.0,
        "ECU": 310.0,
        "USDT_TRC20": 295.0,
        "BTC": 280.0,
        "TRX": 100.0,
    },
    "date": "2026-03-06",
    "hour": 12,
    "minutes": 0,
    "seconds": 0,
}

EMPTY_API_RESPONSE = {
    "tasas": {},
    "date": "2026-03-06",
    "hour": 12,
    "minutes": 0,
    "seconds": 0,
}


def make_mock_response(json_data, status_code=200):
    """Create a mock requests.Response."""
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = json_data
    mock.text = json.dumps(json_data)
    return mock


# ---------------------------------------------------------------------------
# Tests: Token loading
# ---------------------------------------------------------------------------

class TestLoadToken:
    def test_load_from_env(self, monkeypatch):
        monkeypatch.setenv("ELTOQUE_API_TOKEN", SAMPLE_TOKEN)
        assert load_token() == SAMPLE_TOKEN

    def test_load_from_file(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ELTOQUE_API_TOKEN", raising=False)
        token_file = tmp_path / "token.txt"
        token_file.write_text(SAMPLE_TOKEN)
        result = load_token(str(token_file))
        assert result == SAMPLE_TOKEN

    def test_load_from_rtf(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ELTOQUE_API_TOKEN", raising=False)
        token_file = tmp_path / "claveapi.rtf"
        rtf_content = (
            r"{\rtf1\ansi some rtf stuff "
            + SAMPLE_TOKEN
            + r" more stuff}"
        )
        token_file.write_text(rtf_content)
        result = load_token(str(token_file))
        assert result == SAMPLE_TOKEN

    def test_missing_file_exits(self, monkeypatch):
        monkeypatch.delenv("ELTOQUE_API_TOKEN", raising=False)
        with pytest.raises(SystemExit):
            load_token("/nonexistent/path/token.txt")


# ---------------------------------------------------------------------------
# Tests: API Client
# ---------------------------------------------------------------------------

class TestElToqueClient:
    @patch("export.requests.Session")
    def test_fetch_day_success(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.return_value = make_mock_response(SAMPLE_API_RESPONSE)

        client = ElToqueClient(SAMPLE_TOKEN, delay=0)
        result = client.fetch_day(date(2024, 6, 15))

        assert result["tasas"]["USD"] == 300.0
        assert "MLC" in result["tasas"]

    @patch("export.requests.Session")
    def test_fetch_day_empty(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session
        mock_session.get.return_value = make_mock_response(EMPTY_API_RESPONSE)

        client = ElToqueClient(SAMPLE_TOKEN, delay=0)
        result = client.fetch_day(date(2020, 1, 1))

        assert result["tasas"] == {}

    @patch("export.requests.Session")
    def test_fetch_day_rate_limit_then_success(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        # First call returns 429, second returns 200
        mock_session.get.side_effect = [
            make_mock_response({"error": "rate limited"}, 429),
            make_mock_response(SAMPLE_API_RESPONSE),
        ]

        client = ElToqueClient(SAMPLE_TOKEN, delay=0)
        result = client.fetch_day(date(2024, 6, 15))

        assert result["tasas"]["USD"] == 300.0
        assert mock_session.get.call_count == 2


# ---------------------------------------------------------------------------
# Tests: CSV Writer
# ---------------------------------------------------------------------------

class TestWriteCSV:
    def test_basic_csv(self, tmp_path):
        rows = [
            {"date": "2024-01-01", "USD": 300.0, "MLC": 220.0, "USDT_TRC20": 295.0},
            {"date": "2024-01-02", "USD": 305.0, "MLC": 225.0, "USDT_TRC20": 298.0},
        ]
        out = tmp_path / "test.csv"
        write_csv(rows, str(out))

        assert out.exists()
        with open(out) as f:
            reader = csv.DictReader(f)
            data = list(reader)

        assert len(data) == 2
        assert data[0]["date"] == "2024-01-01"
        assert float(data[0]["USD"]) == 300.0
        # Check spread columns
        assert "spread_USD_MLC" in reader.fieldnames
        assert float(data[0]["spread_USD_MLC"]) == 80.0

    def test_empty_rows(self, tmp_path, capsys):
        out = tmp_path / "empty.csv"
        write_csv([], str(out))
        captured = capsys.readouterr()
        assert "No data" in captured.out

    def test_partial_currencies(self, tmp_path):
        rows = [
            {"date": "2021-01-01", "USD": 40.0, "ECU": 46.0},
            {"date": "2021-01-02", "USD": 41.0, "ECU": 47.0, "MLC": 38.0},
        ]
        out = tmp_path / "partial.csv"
        write_csv(rows, str(out))

        with open(out) as f:
            reader = csv.DictReader(f)
            data = list(reader)

        # First row should have empty MLC
        assert data[0]["MLC"] == ""
        assert float(data[1]["MLC"]) == 38.0
        # Check spread_USD_EUR present and correct
        assert "spread_USD_EUR" in reader.fieldnames
        assert float(data[0]["spread_USD_EUR"]) == -6.0
        assert float(data[1]["spread_USD_EUR"]) == -6.0
        # Check EUR_USD_pair present and correct (ECU / USD = EUR/USD cross rate)
        assert "EUR_USD_pair" in reader.fieldnames
        assert abs(float(data[0]["EUR_USD_pair"]) - 46.0 / 40.0) < 1e-4
        assert abs(float(data[1]["EUR_USD_pair"]) - 47.0 / 41.0) < 1e-4


# ---------------------------------------------------------------------------
# Tests: Date Parser
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_valid_date(self):
        assert parse_date("2024-06-15") == date(2024, 6, 15)

    def test_invalid_date(self):
        with pytest.raises(Exception):
            parse_date("not-a-date")

    def test_wrong_format(self):
        with pytest.raises(Exception):
            parse_date("15/06/2024")


# ---------------------------------------------------------------------------
# Tests: Extraction
# ---------------------------------------------------------------------------

class TestExtractHistory:
    @patch("export.requests.Session")
    def test_three_day_extraction(self, mock_session_cls):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        # Return data for 3 consecutive days
        responses = []
        for i in range(3):
            resp_data = {
                "tasas": {"USD": 300.0 + i, "MLC": 220.0 + i},
                "date": "2026-03-06",
                "hour": 12,
                "minutes": 0,
                "seconds": 0,
            }
            responses.append(make_mock_response(resp_data))

        mock_session.get.side_effect = responses

        client = ElToqueClient(SAMPLE_TOKEN, delay=0)
        start = date(2024, 6, 1)
        end = date(2024, 6, 3)

        rows = extract_history(client, start, end)

        assert len(rows) == 3
        assert rows[0]["date"] == "2024-06-01"
        assert rows[0]["USD"] == 300.0
        assert rows[2]["USD"] == 302.0

    @patch("export.requests.Session")
    def test_checkpoint_save_and_resume(self, mock_session_cls, tmp_path):
        mock_session = MagicMock()
        mock_session_cls.return_value = mock_session

        # Pre-save a checkpoint with 2 days
        checkpoint = tmp_path / ".test_checkpoint.json"
        saved_data = {
            "rows": [
                {"date": "2024-06-01", "USD": 300.0},
                {"date": "2024-06-02", "USD": 301.0},
            ],
            "currencies": ["USD"],
        }
        checkpoint.write_text(json.dumps(saved_data))

        # Only need to return 1 more day (June 3)
        mock_session.get.return_value = make_mock_response({
            "tasas": {"USD": 302.0},
            "date": "2026-03-06",
            "hour": 12,
            "minutes": 0,
            "seconds": 0,
        })

        client = ElToqueClient(SAMPLE_TOKEN, delay=0)
        rows = extract_history(
            client,
            date(2024, 6, 1),
            date(2024, 6, 3),
            checkpoint_path=checkpoint,
        )

        assert len(rows) == 3
        # Only 1 API call needed (June 3)
        assert mock_session.get.call_count == 1
