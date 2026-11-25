"""Distributions with no retrievable history are skipped, not fatal."""

import json

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError

from tap_qualtrics.streams import QualtricsNextPagePaginator
from tap_qualtrics.tap import TapQualtrics

CONFIG = {"api_token": "test-token", "survey_ids": "SV_test"}
URL = "https://pdx1.qualtrics.com/API/v3/distributions/EMD_gone/history"


def _response(status: int, body: dict) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()  # noqa: SLF001
    response.request = requests.Request("GET", URL).prepare()
    return response


def test_history_404_yields_no_records_and_does_not_raise() -> None:
    """Qualtrics' CP_GW_LD_NOT_FOUND 404 is treated as an empty history."""
    stream = TapQualtrics(config=CONFIG).streams["distribution_history"]
    not_found = _response(
        404,
        {
            "meta": {
                "httpStatus": "404 - Not Found",
                "error": {"errorCode": "CP_GW_LD_NOT_FOUND"},
            }
        },
    )

    stream.validate_response(not_found)
    assert list(stream.parse_response(not_found)) == []
    assert QualtricsNextPagePaginator().get_next_url(not_found) is None


def test_history_success_still_parses_elements() -> None:
    """A normal history page still yields its elements."""
    stream = TapQualtrics(config=CONFIG).streams["distribution_history"]
    element = {"distributionId": "EMD_ok", "contactId": "CID_1", "status": "Success"}
    ok = _response(
        200, {"result": {"elements": [element], "nextPage": None}, "meta": {}}
    )

    stream.validate_response(ok)
    assert list(stream.parse_response(ok)) == [element]


def test_history_test_email_400_is_skipped() -> None:
    """TestEmail distributions return 400 EMAIL_DISTRIBUTION_NO_RECIPIENTS."""
    stream = TapQualtrics(config=CONFIG).streams["distribution_history"]
    test_email = _response(
        400,
        {
            "meta": {
                "httpStatus": "400 - Bad Request",
                "error": {"errorCode": "EMAIL_DISTRIBUTION_NO_RECIPIENTS"},
            }
        },
    )

    stream.validate_response(test_email)
    assert list(stream.parse_response(test_email)) == []


def test_other_400_still_fails_the_stream() -> None:
    """Only known no-history errors are skipped; other 400s surface."""
    stream = TapQualtrics(config=CONFIG).streams["distribution_history"]
    bad_request = _response(
        400,
        {"meta": {"httpStatus": "400 - Bad Request", "error": {"errorCode": "OTHER"}}},
    )

    with pytest.raises(FatalAPIError):
        stream.validate_response(bad_request)
