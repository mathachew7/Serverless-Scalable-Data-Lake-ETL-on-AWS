import json
import os
import sys
import pytest
from unittest.mock import MagicMock

# conftest.py sets AWS_DEFAULT_REGION + fake creds before this import
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../lambda/ingestion_trigger"))

import handler  # single import; we swap handler.s3 / handler.glue per test


def _s3_event(bucket="test-raw-bucket", key="uploads/orders.csv", size=1024):
    return {
        "Records": [{
            "s3": {
                "bucket": {"name": bucket},
                "object": {"key": key, "size": size},
            }
        }]
    }


def _patch(glue_run_id="jr-000"):
    """Return (mock_s3, mock_glue) and inject them into the module."""
    mock_s3   = MagicMock()
    mock_glue = MagicMock()
    mock_glue.start_job_run.return_value = {"JobRunId": glue_run_id}
    handler.s3   = mock_s3
    handler.glue = mock_glue
    return mock_s3, mock_glue


# ── Happy path ────────────────────────────────────────────────────────────────

def test_csv_triggers_glue():
    mock_s3, mock_glue = _patch("jr-csv-001")

    result = handler.lambda_handler(_s3_event(key="uploads/orders.csv"), {})

    assert result["statusCode"] == 200
    body = json.loads(result["body"])
    assert body[0]["status"]        == "triggered"
    assert body[0]["format"]        == "csv"
    assert body[0]["glue_run_id"]   == "jr-csv-001"
    assert body[0]["organized_key"].startswith("raw/csv/")
    mock_s3.copy_object.assert_called_once()
    mock_glue.start_job_run.assert_called_once()


def test_json_triggers_glue():
    mock_s3, mock_glue = _patch("jr-json-002")

    result = handler.lambda_handler(_s3_event(key="uploads/events.json"), {})

    body = json.loads(result["body"])
    assert body[0]["format"] == "json"
    assert body[0]["organized_key"].startswith("raw/json/")


def test_parquet_triggers_glue():
    mock_s3, mock_glue = _patch("jr-parquet-003")

    result = handler.lambda_handler(_s3_event(key="uploads/snapshot.parquet"), {})

    body = json.loads(result["body"])
    assert body[0]["format"] == "parquet"


def test_organized_key_has_hive_partitions():
    mock_s3, mock_glue = _patch()

    result = handler.lambda_handler(_s3_event(key="uploads/data.csv"), {})

    key = json.loads(result["body"])[0]["organized_key"]
    assert "year="  in key
    assert "month=" in key
    assert "day="   in key
    assert key.endswith("data.csv")


def test_glue_called_with_organized_path():
    mock_s3, mock_glue = _patch()

    handler.lambda_handler(_s3_event(key="uploads/orders.csv"), {})

    args = mock_glue.start_job_run.call_args
    input_key = args[1]["Arguments"]["--INPUT_KEY"]
    assert input_key.startswith("raw/csv/")


def test_multiple_records_all_processed():
    mock_s3, mock_glue = MagicMock(), MagicMock()
    mock_glue.start_job_run.side_effect = [{"JobRunId": "jr-001"}, {"JobRunId": "jr-002"}]
    handler.s3 = mock_s3
    handler.glue = mock_glue

    event = {
        "Records": [
            {"s3": {"bucket": {"name": "test-raw-bucket"}, "object": {"key": "uploads/orders.csv",  "size": 500}}},
            {"s3": {"bucket": {"name": "test-raw-bucket"}, "object": {"key": "uploads/events.json", "size": 300}}},
        ]
    }
    result = handler.lambda_handler(event, {})
    body   = json.loads(result["body"])

    assert len(body)                        == 2
    assert mock_glue.start_job_run.call_count == 2
    assert mock_s3.copy_object.call_count     == 2


# ── Skipped files ─────────────────────────────────────────────────────────────

def test_unsupported_extension_skipped():
    mock_s3, mock_glue = _patch()

    result = handler.lambda_handler(_s3_event(key="uploads/report.xlsx"), {})

    body = json.loads(result["body"])
    assert body[0]["status"] == "skipped"
    assert body[0]["reason"] == "unsupported_extension"
    mock_glue.start_job_run.assert_not_called()
    mock_s3.copy_object.assert_not_called()


def test_empty_file_skipped():
    mock_s3, mock_glue = _patch()

    result = handler.lambda_handler(_s3_event(key="uploads/empty.csv", size=0), {})

    body = json.loads(result["body"])
    assert body[0]["status"] == "skipped"
    assert body[0]["reason"] == "empty_file"
    mock_glue.start_job_run.assert_not_called()


# ── Format detection (pure unit) ──────────────────────────────────────────────

@pytest.mark.parametrize("key,expected", [
    ("uploads/data.csv",      "csv"),
    ("uploads/data.CSV",      "csv"),
    ("uploads/events.json",   "json"),
    ("uploads/events.jsonl",  "json"),
    ("uploads/snap.parquet",  "parquet"),
    ("uploads/data.txt",      "unknown"),
])
def test_detect_format(key, expected):
    assert handler._detect_format(key) == expected
