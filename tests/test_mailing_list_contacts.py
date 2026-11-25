"""mailing_list_contacts: list config parsing and which errors are skipped."""

from __future__ import annotations

import json

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError

from tap_qualtrics.tap import TapQualtrics

URL = (
    "https://pdx1.qualtrics.com/API/v3/directories/POOL_1/mailinglists/CG_1/contacts"
)


def _stream(mailing_lists: str | None = "POOL_1/CG_1, POOL_1/CG_2"):
    config = {"api_token": "test-token", "survey_ids": "SV_test"}
    if mailing_lists is not None:
        config["mailing_lists"] = mailing_lists
    return TapQualtrics(config=config).streams["mailing_list_contacts"]


def _response(status: int, code: str) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(  # noqa: SLF001
        {"meta": {"httpStatus": str(status), "error": {"errorCode": code}}}
    ).encode()
    response.request = requests.Request("GET", URL).prepare()
    return response


def test_one_partition_per_configured_list() -> None:
    """Each directoryId/mailingListId pair becomes its own partition."""
    assert _stream().partitions == [
        {"directory_id": "POOL_1", "mailing_list_id": "CG_1"},
        {"directory_id": "POOL_1", "mailing_list_id": "CG_2"},
    ]


def test_unconfigured_lists_read_nothing() -> None:
    """Without mailing_lists the stream has no partitions and yields no records."""
    stream = _stream(None)
    assert stream.partitions is None
    assert list(stream.get_records(None)) == []


def test_malformed_list_entry_is_rejected() -> None:
    """An entry missing its directory fails loudly instead of building a bad URL."""
    with pytest.raises(ValueError, match="POOL_"):
        _ = _stream("CG_1").partitions


def test_list_not_visible_is_skipped() -> None:
    """A list not shared with the API user (404) yields nothing instead of failing."""
    stream = _stream()
    not_found = _response(404, "LIST_CONTACTS_IN_LIST_NOT_FOUND")

    stream.validate_response(not_found)
    assert list(stream.parse_response(not_found)) == []


def test_unauthorized_list_still_fails() -> None:
    """A visible list the user may not read (401) surfaces as an error."""
    with pytest.raises(FatalAPIError):
        _stream().validate_response(
            _response(401, "LIST_CONTACTS_IN_LIST_UNAUTHORIZED")
        )
