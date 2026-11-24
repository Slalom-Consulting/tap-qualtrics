"""History is requested only for sent distributions changed since the bookmark."""

from tap_qualtrics.tap import TapQualtrics

SURVEY = "SV_test"
CONFIG = {"api_token": "test-token", "survey_ids": SURVEY}
CONTEXT = {"survey_id": SURVEY}
BOOKMARK = "2026-11-24T12:00:00Z"
STATE = {
    "bookmarks": {
        "distributions": {
            "partitions": [
                {
                    "context": CONTEXT,
                    "replication_key": "modifiedDate",
                    "replication_key_value": BOOKMARK,
                }
            ]
        }
    }
}


def _children(record: dict) -> list:
    stream = TapQualtrics(config=CONFIG, state=STATE).streams["distributions"]
    # The SDK resolves the bookmark at the start of each partition's sync.
    stream._write_starting_replication_value(CONTEXT)  # noqa: SLF001
    return list(stream.generate_child_contexts(record, CONTEXT))


def test_changed_sent_distribution_requests_history() -> None:
    """A Done distribution modified after the bookmark gets its history synced."""
    record = {
        "id": "EMD_new",
        "requestStatus": "Done",
        "modifiedDate": "2026-11-24T12:00:01Z",
    }
    assert _children(record) == [{"survey_id": SURVEY, "distribution_id": "EMD_new"}]


def test_unchanged_distribution_is_skipped() -> None:
    """A distribution not modified since the bookmark is not re-requested."""
    record = {"id": "EMD_old", "requestStatus": "Done", "modifiedDate": BOOKMARK}
    assert _children(record) == []


def test_pending_distribution_is_skipped() -> None:
    """An unsent distribution has no history, so no request is made."""
    record = {
        "id": "EMD_pending",
        "requestStatus": "Pending",
        "modifiedDate": "2026-11-25T00:00:00Z",
    }
    assert _children(record) == []
