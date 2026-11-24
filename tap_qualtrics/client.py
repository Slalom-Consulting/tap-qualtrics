"""Custom client handling, including QualtricsStream base class."""

from __future__ import annotations

import typing as t

import base64
import urllib.parse
import sys
import logging
from functools import cached_property
from typing import Any, Callable, Iterable

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import JSONPathPaginator # noqa: TCH002
from singer_sdk.streams import RESTStream
from singer_sdk import Tap, Stream
from singer_sdk.helpers._state import (
    get_state_partitions_list
)

from singer_sdk.authenticators import APIKeyAuthenticator

logging.basicConfig(level=logging.INFO)

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

class QualtricsStream(RESTStream):
    """Qualtrics stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("url_base")

    records_jsonpath = "$[*]"

    @property
    def authenticator(self):
        return APIKeyAuthenticator(stream=self, 
                                   key="x-api-token",
                                   value=self.config.get("api_token"),
                                   location="header",)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        headers["content-type"] = 'application/json'
        headers["x-api-token"] = self.config.get("api_token", "")

        return headers

    @property
    def survey_ids(self) -> list[str]:
        """Get survey IDs as a list, parsing from comma-separated string."""
        survey_ids_config = self.config.get("survey_ids")
        
        if isinstance(survey_ids_config, str):
            return [survey_id.strip() for survey_id in survey_ids_config.split(",") if survey_id.strip()]
        elif isinstance(survey_ids_config, list):
            return survey_ids_config
        else:
            raise ValueError(f"survey_ids must be either a list or a comma-separated string, got {type(survey_ids_config)}")
    
    @property
    def partitions(self) -> list[dict] | None:
        """Get stream partitions.

        Developers may override this property to provide a default partitions list.

        By default, this method returns a list of any partitions which are already
        defined in state, otherwise None.

        Returns:
            A list of partition key dicts (if applicable), otherwise `None`.
        """
        existing_partitions = get_state_partitions_list(self.tap_state, self.name)
        
        if existing_partitions:
            partition_result = [partition_state["context"] for partition_state in existing_partitions]
        else:
            partition_result = []
            
        # Get survey IDs from tap (no parentheses - it's a property!)
        config_survey_ids = self.survey_ids
        config_survey_result = [{"survey_id": survey_id} for survey_id in config_survey_ids]

        for survey_dict in config_survey_result:
            if survey_dict not in partition_result:
                partition_result.append(survey_dict)

        logging.info(f'Partitions: {partition_result}')
        return partition_result if partition_result else None

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return records, skipping any that lack a source primary key value.

        Qualtrics can export content-less partial responses with no ``responseId``.
        Targets reject records without a primary key and abort the whole sync, so a
        single such record would fail every run while it exists in the source. Keys
        supplied by the partition context (``survey_id``) are filled in later by
        ``post_process`` and are not checked here.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            Each record that carries a value for every source primary key.
        """
        source_keys = [k for k in self.primary_keys or [] if k not in (context or {})]
        for record in super().get_records(context):
            missing = [k for k in source_keys if record.get(k) in (None, "")]
            if missing:
                # Log keys only; response contents are sensitive.
                self.logger.warning(
                    "Skipping %s record with no value for primary key(s) %s "
                    "(context: %s)",
                    self.name,
                    missing,
                    context,
                )
                continue
            yield record



    


     