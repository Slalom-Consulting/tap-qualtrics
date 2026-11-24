"""Records without a source primary key must be skipped, not emitted."""

import pytest

from tap_qualtrics.tap import TapQualtrics

CONFIG = {"api_token": "test-token", "survey_ids": "SV_test"}
CONTEXT = {"survey_id": "SV_test"}


def test_in_progress_stream_skips_records_without_response_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A content-less in-progress export must not reach the target."""
    stream = TapQualtrics(config=CONFIG).streams["survey_responses_in_progress"]
    valid = {"responseId": "R_valid", "values": {"finished": 0}}
    exported = [
        valid,
        {},  # content-less partial response, as seen in the 2024 client survey
        {"responseId": None, "values": {}},
        {"responseId": "", "values": {}},
    ]
    monkeypatch.setattr(stream, "request_records", lambda _context: iter(exported))

    assert list(stream.get_records(CONTEXT)) == [valid]
