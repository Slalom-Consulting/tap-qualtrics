"""Qualtrics tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_qualtrics import streams


class TapQualtrics(Tap):
    """Qualtrics tap class."""

    name = "tap-qualtrics"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_token",
            th.StringType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
            title="API Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "survey_ids",
            th.ArrayType(th.StringType),
            required=True,
            title="Survey IDs",
            description="Survey IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType(nullable=True),
            description="The earliest record date to sync",
        ),
        th.Property(
            "url_base",
            th.StringType(nullable=False),
            title="Base URL",
            default="https://pdx1.qualtrics.com",
        )
    ).to_dict()

    def discover_streams(self) -> list[streams.qualtricsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            # streams.SurveyResponsesStream(self),
            # streams.SurveyResponsesInProgressStream(self),
            streams.SurveyQuestionsStream(self),
            # streams.SurveyDefinitionStream(self)
        ]


if __name__ == "__main__":
    TapQualtrics.cli()
