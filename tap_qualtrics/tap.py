"""Qualtrics tap class."""

from __future__ import annotations
import json

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
            th.CustomType({
                "anyOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "string"}
                ]
            }),
            required=True,
            title="Survey IDs",
            description="Survey IDs to replicate (can be an array of strings or a JSON string)",
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
        ),
        th.Property(
            "max_file_ready_attempts",
            th.StringType(nullable=True),
            description="The maximum number of attempts to check if a file is ready",
            default="3"
        ),
        th.Property(
            "initial_wait_seconds",
            th.StringType(nullable=True),
            description="The initial wait time before checking if a file is ready",
            default="5"
        ),
        th.Property(
            "retry_wait_seconds",
            th.StringType(nullable=True),
            description="The wait time between retry attempts to check if a file is ready",
            default="10"
        )
    ).to_dict()

    @property
    def survey_ids(self) -> list[str]:
        """Get survey IDs as a list, parsing from JSON string if necessary."""
        survey_ids_config = self.config.get("survey_ids")
        
        if isinstance(survey_ids_config, str):
            try:
                return json.loads(survey_ids_config)
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse survey_ids JSON string: {survey_ids_config}")
                raise ValueError(f"Invalid JSON format for survey_ids: {e}")
        elif isinstance(survey_ids_config, list):
            return survey_ids_config
        else:
            raise ValueError(f"survey_ids must be either a list or a JSON string, got {type(survey_ids_config)}")

    def discover_streams(self) -> list[streams.QualtricsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SurveyResponsesStream(self),
            streams.SurveyResponsesInProgressStream(self),
            streams.SurveyQuestionsStream(self),
            streams.SurveyDefinitionsStream(self)
        ]


if __name__ == "__main__":
    TapQualtrics.cli()
