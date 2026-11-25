"""Stream type classes for tap-qualtrics."""

from __future__ import annotations

import time
import typing as t
import copy
from importlib import resources
import logging
import requests
import zipfile
import io
import datetime
import json
from typing import Iterable, Mapping
from urllib.parse import ParseResult, parse_qsl

from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.pagination import BaseHATEOASPaginator

from tap_qualtrics.client import QualtricsStream

SCHEMAS_DIR = resources.files(__package__) / "schemas"

StringOrIntegerType = th.CustomType({
    "type": ["string", "integer"]
})

BooleanOrStringType = th.CustomType({
    "type": ["boolean", "string"]
})


class SurveyResponsesStream(QualtricsStream):
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._current_context = None
    
    def get_url(self, context: dict | None) -> str:
        """Get stream entity URL.

        Developers override this method to perform dynamic URL generation.

        Args:
            context: Stream partition or context dictionary.

        Returns:
            A URL, optionally targeted to a specific partition or context.
        """
        url = "".join([self.url_base, self.path or ""])
        vals = copy.copy(dict(self.config))
        vals.update(context or {})
        for k, v in vals.items():
            search_text = f"{{{k}}}"
            if search_text in url:
                url = url.replace(search_text, self._url_encode(v))
        self._current_context = vals.get('survey_id')
        logging.info(f'CURRENT CONTEXT: {self._current_context}')
        return url
    
    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> (
        Iterable[bytes]
        | str
        | bytes
        | list[tuple[t.Any, t.Any]]
        | tuple[tuple[t.Any, t.Any]]
        | Mapping[str, t.Any]
        | None
    ):
        """Prepare the data payload for the HTTP request.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        payload: dict = {}
        payload['sortByLastModifiedDate'] = "true"
        starting_date = self.get_starting_timestamp(context) or self.config.get('start_date')
        payload['startDate'] = starting_date.isoformat() if starting_date else None
        payload['format'] = "ndjson"
        self.logger.info(f"Request payload: {payload}")

        return payload

    def parse_response(self, response:requests.Response) -> t.Iterable[dict]:
        """Parse the response from the API.

        Args:
            response: The HTTP response object.

        Returns:
            An iterable of records parsed from the response.
        """
        data = self._validate_initial_response(response)
        progress_id = self._extract_progress_id(data)
        file_id = self._wait_for_file_ready(progress_id)
        return self._download_and_extract_responses(file_id)

    def _validate_initial_response(self, response: requests.Response) -> dict:
        if response.status_code != 200:
            self.logger.error(f"Failed to create survey responses export file: {response.status_code}")
            raise ValueError(f"Invalid response from API: {response.status_code}")

        try:
            data = response.json()
        except ValueError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")
            raise ValueError("Invalid JSON response from API")
            
        return data

    def _extract_progress_id(self, data: dict) -> str | None:
        return data.get('result', {}).get('progressId')

    def _wait_for_file_ready(self, progress_id: str | None) -> str | None:
        if not progress_id:
            self.logger.error("No progress ID found.")
            return None

        try:
            max_attempts = self.config.get("max_file_ready_attempts", 3)
            initial_wait = self.config.get("initial_wait_seconds", 5)
            retry_wait = self.config.get("retry_wait_seconds", 10)
            
            # Ensure they're integers
            if not isinstance(max_attempts, int):
                max_attempts = int(max_attempts)
            if not isinstance(initial_wait, int):
                initial_wait = int(initial_wait)
            if not isinstance(retry_wait, int):
                retry_wait = int(retry_wait)
                
        except (ValueError, TypeError) as e:
            self.logger.error(f"Invalid configuration values: {e}")
            return None

        attempt = 1
        time.sleep(initial_wait) 

        while attempt <= max_attempts:
            self.logger.info(f"Attempt {attempt}/{max_attempts} to check if the file is ready.")

            file_id = self.is_file_ready(progress_id)
            if file_id:
                self.logger.info("File is ready for download.")
                return file_id

            self.logger.info("File not ready yet, waiting before retrying...")
            attempt += 1
            time.sleep(retry_wait)
        self.logger.error(f"File not ready after {max_attempts} attempts.")
        return None

    def _download_and_extract_responses(self, file_id: str | None) -> dict | None:
        # Now we can download the file
        self.logger.info("File is ready, downloading file")
        responses_data = self.download_file(file_id)
        if not responses_data:
            self.logger.error("Failed to download the responses file.")
            return None

        self.logger.info(f"Downloaded and parsed responses data")
        
        # Extract the actual survey responses
        responses = responses_data.get('responses', [])
        self.logger.info(f"Found {len(responses)} survey responses")
        
        return responses

    def download_file(self, file_id: str) -> dict | None:
        """Download the file using the file ID."""
        url = f"{self.url_base}/API/v3/surveys/{self._current_context}/export-responses/{file_id}/file"
        
        try:
            self.logger.info(f"Downloading file from {url}")
            response = requests.get(url, headers=self.http_headers)
            response.raise_for_status()
            
            return self._extract_ndjson_from_zip(response.content)
                
        except requests.RequestException as e:
            self.logger.error(f"Error downloading file: {e}")
            return None

    def _extract_ndjson_from_zip(self, zip_content: bytes) -> dict | None:
        """Extract ndjson data from zip file content."""
        try:
            with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
                # List all files in the zip
                file_list = zip_file.namelist()
                self.logger.info(f"Files in zip: {file_list}")
                
                # Look for NDJSON file (usually the first or only file)
                ndjson_file = None
                for filename in file_list:
                    if filename.endswith('.ndjson'):
                        ndjson_file = filename
                        break

                if not ndjson_file and file_list:
                    # If no .ndjson extension found, try the first file
                    ndjson_file = file_list[0]

                if ndjson_file:
                    with zip_file.open(ndjson_file) as f:
                        ndjson_content = f.read().decode('utf-8')
                        return self.parse_ndjson_content(ndjson_content)
                else:
                    self.logger.error("No suitable file found in zip archive")
                    return None
                    
        except zipfile.BadZipFile as e:
            self.logger.error(f"Invalid zip file: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error extracting ndjson from zip: {e}")
            return None

    def parse_ndjson_content(self, ndjson_content: str) -> dict:
        """Parse ndjson content and convert to response format."""
        try:
            responses = []
            
            for line in ndjson_content.strip().split('\n'):
                if line.strip():
                    try:
                        response = json.loads(line)
                        responses.append(response)
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse JSON line: {line[:100]}... Error: {e}")
                        continue
            
            self.logger.info(f"Parsed {len(responses)} responses from ndjson")
            return {'responses': responses}
            
        except Exception as e:
            self.logger.error(f"Error parsing ndjson content: {e}")
            return {'responses': []}

    def is_file_ready(self, progress_id: str) -> str | None:
        """Check if the file is ready for download."""
        url = f"{self.url_base}/API/v3/surveys/{self._current_context}/export-responses/{progress_id}"
        self.logger.info(f"Checking if file is ready for progress ID: {progress_id}")

        headers = self.http_headers
        try: 
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            status = data.get('result', {}).get('status')
            self.logger.info(f"File status: {status}")
            if status == 'complete':
                file_id = data.get('result', {}).get('fileId')
                return file_id
        except requests.RequestException as e:
            self.logger.error(f"Error checking file readiness: {e}")
            return None
        return None

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row['last_modified_date'] = row.get('values', {}).get('_lastModifiedDate')
        row['survey_id'] = self._current_context
        return row

    name = "survey_responses"
    primary_keys = ["responseId","survey_id"]
    replication_key = "last_modified_date"
    is_sorted = True
    path = "/API/v3/surveys/{survey_id}/export-responses"
    rest_method = "POST"
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        # Top-level response identifier
        th.Property("responseId", th.StringType, description="Unique identifier for the survey response"),
        th.Property("last_modified_date", th.DateTimeType, description="Last modified date of the response"),
        th.Property("survey_id", th.StringType, description="Survey ID this response belongs to"),
        
        # Values object - Core response metadata (allows any additional properties)
        th.Property("values", th.ObjectType(
            additional_properties=True
        ), description="Survey response values and metadata"),
        
        # Labels object - Human-readable labels for responses (allows any additional properties)
        th.Property("labels", th.ObjectType(
            additional_properties=True
        ), description="Human-readable labels for response values"),
        
        # Survey display metadata
        th.Property("displayedFields", th.ArrayType(th.StringType), description="Fields that were displayed to the respondent"),
        th.Property("displayedValues", th.ObjectType(
            additional_properties=True
        ), description="Possible values for displayed fields"),
    ).to_dict()

class SurveyResponsesInProgressStream(SurveyResponsesStream):
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: t.Any | None,
    ) -> (
        Iterable[bytes]
        | str
        | bytes
        | list[tuple[t.Any, t.Any]]
        | tuple[tuple[t.Any, t.Any]]
        | Mapping[str, t.Any]
        | None
    ):
        """Prepare the data payload for the HTTP request.
        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        payload: dict = {}
        payload['exportResponsesInProgress'] = "true"
        payload['startDate'] = self.config.get('start_date')
        payload['format'] = "ndjson" 

        return payload
                
    name = "survey_responses_in_progress"
    primary_keys = ["responseId","survey_id"]
    replication_key = None
    rest_method = "POST"
    records_jsonpath = "$[*]"
    schema = th.PropertiesList(
        # Top-level response identifier
        th.Property("responseId", th.StringType, description="Unique identifier for the survey response"),
        th.Property("survey_id", th.StringType, description="Survey ID this response belongs to"),
        
        # Values object - Core response metadata (allows any additional properties)
        th.Property("values", th.ObjectType(
            additional_properties=True
        ), description="Survey response values and metadata"),
        
        # Labels object - Human-readable labels for responses (allows any additional properties)
        th.Property("labels", th.ObjectType(
            additional_properties=True
        ), description="Human-readable labels for response values"),
        
        # Survey display metadata
        th.Property("displayedFields", th.ArrayType(th.StringType), description="Fields that were displayed to the respondent"),
        th.Property("displayedValues", th.ObjectType(
            additional_properties=True
        ), description="Possible values for displayed fields"),
    ).to_dict()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        row['survey_id'] = self._current_context
        return row

class SurveyQuestionsStream(QualtricsStream):
    """Stream for Qualtrics survey questions."""
    
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(self.__class__.__name__)
    

    name = "survey_questions"
    primary_keys = ["QuestionID","survey_id"]
    path = "/API/v3/survey-definitions/{survey_id}/questions"
    rest_method = "GET"
    records_jsonpath = "$[result][elements][*]"

    schema = th.PropertiesList(
        # Core question identifiers
        th.Property("QuestionID", th.StringType, description="Unique identifier for the question"),
        th.Property("survey_id", th.StringType, description="Survey ID this question belongs to"),
        
        # Question metadata
        th.Property("QuestionType", th.StringType, description="Type of question (MC, TE, Matrix, DB, etc.)"),
        th.Property("Selector", th.StringType, description="Question selector/subtype (TB, ML, SACOL, etc.)"),
        th.Property("SubSelector", th.StringType, description="Question sub-selector (TX, etc.)"),
        th.Property("QuestionDescription", th.StringType, description="Question description/title"),
        th.Property("QuestionText", th.StringType, description="The actual question text"),
        th.Property("DataExportTag", th.StringType, description="Data export tag for the question"),
        th.Property("DefaultChoices", th.BooleanType, description="Whether to use default choices"),
        th.Property("DataVisibility", th.ObjectType(
            th.Property("Private", th.BooleanType, description="Private visibility setting"),
            th.Property("Hidden", th.BooleanType, description="Hidden visibility setting"),
            additional_properties=True
        ), description="Data visibility settings"),
        
        # Question configuration
        th.Property("Validation", th.ObjectType(additional_properties=True), description="Question validation settings"),
        th.Property("GradingData", th.ArrayType(th.ObjectType(additional_properties=True)), description="Grading configuration for the question"),
        th.Property("Language", th.ArrayType(th.ObjectType(additional_properties=True)), description="Language-specific question data"),
        th.Property("NextChoiceId", th.IntegerType, description="Next available choice ID"),
        th.Property("NextAnswerId", th.IntegerType, description="Next available answer ID"),
        
        # Question choices/answers - Updated based on actual data structure
        th.Property("Choices", th.ObjectType(additional_properties=True), description="Available choices for the question"),
        th.Property("ChoiceOrder", th.ArrayType(StringOrIntegerType), description="Order of choices"),
        th.Property("Answers", th.ObjectType(additional_properties=True), description="Available answers for matrix questions"),
        th.Property("AnswerOrder", th.ArrayType(StringOrIntegerType), description="Order of answers"),
        
        # Display and behavior settings
        th.Property("RecodeValues", th.ObjectType(additional_properties=True), description="Recode values for choices"),
        th.Property("ChoiceDataExportTags", th.BooleanType, description="Data export tags for choices"),
        th.Property("VariableNaming", th.ObjectType(additional_properties=True), description="Variable naming configuration"),
        th.Property("ColumnSubQuestion", th.BooleanType, description="Whether this is a column sub-question"),
        
        # Question flow and logic
        th.Property("DisplayLogic", th.ObjectType(additional_properties=True), description="Display logic configuration"),
        th.Property("ChoiceRandomization", th.ObjectType(additional_properties=True), description="Choice randomization settings"),
        
        # Additional configuration - Updated based on actual data structure
        th.Property("Configuration", th.ObjectType(additional_properties=True), description="Additional question configuration"),
        
        # Question metadata - Updated based on actual data structure
        th.Property("QuestionInstructionText", th.StringType, description="Instruction text for the question"),
        th.Property("SearchSource", th.ObjectType(additional_properties=True), description="Search source configuration"),
        th.Property("DynamicChoices", th.ObjectType(additional_properties=True), description="Dynamic choices configuration"),
        
        # Experimental or advanced features
        th.Property("AddOnProperties", th.ObjectType(additional_properties=True), description="Add-on properties"),
        th.Property("AnalyzeChoices", th.ObjectType(additional_properties=True), description="Analysis choices configuration"),
        
        # Question grouping and organization
        th.Property("Block", th.StringType, description="Block ID this question belongs to"),
        th.Property("QuestionNumber", th.IntegerType, description="Question number in survey"),
    ).to_dict()

class SurveyDefinitionsStream(QualtricsStream):
    """Stream for Qualtrics survey definitions."""
    
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(self.__class__.__name__)
    

    name = "survey_definitions"
    primary_keys = ["SurveyID"]
    path = "/API/v3/survey-definitions/{survey_id}"
    rest_method = "GET"
    records_jsonpath = "$[result][*]"

    schema = th.PropertiesList(
        # Core survey identifiers
        th.Property("SurveyID", th.StringType, description="Unique identifier for the survey"),
        th.Property("survey_id", th.StringType, description="Survey ID this response belongs to"),
        th.Property("SurveyName", th.StringType, description="Name of the survey"),
        th.Property("SurveyStatus", th.StringType, description="Status of the survey (Active, Inactive, etc.)"),
        th.Property("BrandID", th.StringType, description="Brand ID associated with the survey"),
        th.Property("OwnerID", th.StringType, description="Owner ID of the survey"),
        th.Property("CreatorID", th.StringType, description="Creator ID of the survey"),
        th.Property("BrandBaseURL", th.StringType, description="Base URL for the brand"),
        
        # Survey metadata
        th.Property("QuestionCount", StringOrIntegerType, description="Total number of questions in the survey"),
        th.Property("LastModified", th.DateTimeType, description="Last modified date of the survey"),
        th.Property("LastAccessed", th.DateTimeType, description="Last accessed date of the survey"),
        th.Property("LastActivated", th.DateTimeType, description="Last activated date of the survey"),
        
        # Top-level objects with flexible structure
        th.Property("SurveyOptions", th.ObjectType(
            additional_properties=True
        ), description="Survey options and configuration"),
        
        th.Property("Questions", th.ObjectType(
            additional_properties=True
        ), description="All questions in the survey"),
        
        th.Property("Blocks", th.ObjectType(
            additional_properties=True
        ), description="Survey blocks configuration"),
        
        th.Property("ResponseSets", th.ObjectType(
            additional_properties=True
        ), description="Response sets configuration"),
        
        th.Property("SurveyFlow", th.ObjectType(
            additional_properties=True
        ), description="Survey flow configuration"),
        
        th.Property("Scoring", th.ObjectType(
            additional_properties=True
        ), description="Survey scoring configuration"),
        
        th.Property("ProjectInfo", th.ObjectType(
            additional_properties=True
        ), description="Project information"),
        
    ).to_dict()


class QualtricsNextPagePaginator(BaseHATEOASPaginator):
    """Follow the absolute ``result.nextPage`` URL that list endpoints return."""

    def get_next_url(self, response: requests.Response) -> str | None:
        return response.json().get("result", {}).get("nextPage")


class QualtricsPaginatedStream(QualtricsStream):
    """Base for list endpoints paginated through ``result.nextPage``."""

    rest_method = "GET"
    records_jsonpath = "$.result.elements[*]"

    def get_new_paginator(self) -> QualtricsNextPagePaginator:
        return QualtricsNextPagePaginator()

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: ParseResult | None,
    ) -> dict[str, t.Any]:
        # nextPage already carries every query parameter, including skipToken.
        if next_page_token:
            return dict(parse_qsl(next_page_token.query))
        return self.get_first_page_params(context)

    def get_first_page_params(self, context: dict | None) -> dict[str, t.Any]:  # noqa: ARG002
        return {}


class DistributionsStream(QualtricsPaginatedStream):
    """Email distributions (invites, reminders, thank-yous) sent for a survey.

    The list endpoint has no modified-date filter, so every run lists all of a
    survey's distributions. ``modifiedDate`` is the replication key only so the
    history child stream can skip distributions unchanged since the last run.
    """

    name = "distributions"
    path = "/API/v3/distributions"
    primary_keys = ["id"]
    replication_key = "modifiedDate"
    is_sorted = False

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Distribution ID (EMD_...)"),
        th.Property("survey_id", th.StringType, description="Survey ID this distribution belongs to"),
        th.Property("parentDistributionId", th.StringType, description="Initial invite this reminder or thank-you follows"),
        th.Property("ownerId", th.StringType, description="User ID of the distribution owner"),
        th.Property("organizationId", th.StringType, description="Organization or brand ID"),
        th.Property("requestStatus", th.StringType, description="Pending, Done, Generated, or Error"),
        th.Property("requestType", th.StringType, description="Invite, Reminder, ThankYou, or GeneratedInvite"),
        th.Property("sendDate", th.DateTimeType, description="When the distribution was or will be sent"),
        th.Property("createdDate", th.DateTimeType, description="When the distribution was created"),
        th.Property("modifiedDate", th.DateTimeType, description="When the distribution was last modified"),
        th.Property("customHeaders", th.ObjectType(additional_properties=True), description="Custom email headers"),
        th.Property("headers", th.ObjectType(additional_properties=True), description="Sender and subject of the email"),
        th.Property("recipients", th.ObjectType(
            th.Property("mailingListId", th.StringType),
            th.Property("contactId", th.StringType),
            th.Property("libraryId", th.StringType),
            th.Property("sampleId", th.StringType),
            additional_properties=True,
        ), description="Contact, mailing list, or sample the distribution targets"),
        th.Property("message", th.ObjectType(additional_properties=True), description="Library message used for the email body"),
        th.Property("surveyLink", th.ObjectType(
            th.Property("surveyId", th.StringType),
            th.Property("expirationDate", th.DateTimeType),
            th.Property("linkType", th.StringType),
            additional_properties=True,
        ), description="Survey link settings for the distribution"),
        th.Property("embeddedData", th.ObjectType(additional_properties=True), description="Embedded data sent with the distribution"),
        th.Property("stats", th.ObjectType(
            *(th.Property(k, th.IntegerType) for k in (
                "sent", "failed", "started", "bounced", "opened",
                "skipped", "finished", "complaints", "blocked",
            )),
            additional_properties=True,
        ), description="Email and survey event counts for the distribution"),
    ).to_dict()

    def get_first_page_params(self, context: dict | None) -> dict[str, t.Any]:
        return {"surveyId": (context or {})["survey_id"]}

    def generate_child_contexts(
        self,
        record: dict,
        context: dict | None,
    ) -> Iterable[dict | None]:
        """Request history only where it can exist and may have changed.

        Pending distributions have not been sent, so their history is empty.
        Others are skipped when unmodified since the bookmark (or start_date).
        """
        if record.get("requestStatus") == "Pending":
            return
        since = self.get_starting_timestamp(context)
        modified = record.get("modifiedDate")
        if since and modified and datetime.datetime.fromisoformat(
            modified.replace("Z", "+00:00")
        ) <= since:
            return
        yield {"survey_id": (context or {})["survey_id"], "distribution_id": record["id"]}


class DistributionHistoryStream(QualtricsPaginatedStream):
    """Per-contact delivery and response history of a distribution.

    Links a contact to its survey session (``surveySessionId``, the FS_ ID of
    in-progress responses) and completed response (``responseId``).
    """

    name = "distribution_history"
    parent_stream_type = DistributionsStream
    path = "/API/v3/distributions/{distribution_id}/history"
    primary_keys = ["distributionId", "contactId"]
    replication_key = None
    # Keep state per survey rather than one entry per distribution.
    state_partitioning_keys = ["survey_id"]

    schema = th.PropertiesList(
        th.Property("distributionId", th.StringType, description="Distribution ID (EMD_...)"),
        th.Property("contactId", th.StringType, description="XM Directory contact ID (CID_...)"),
        th.Property("survey_id", th.StringType, description="Survey ID the distribution belongs to"),
        th.Property("contactLookupId", th.StringType, description="Mailing-list lookup ID of the contact"),
        th.Property("contactFrequencyRuleId", th.StringType, description="Contact frequency rule that applied, if any"),
        th.Property("status", th.StringType, description="Latest delivery or survey status, for example Success or SurveyFinished"),
        th.Property("sentAt", th.DateTimeType, description="When the email was sent"),
        th.Property("openedAt", th.DateTimeType, description="When the email was opened"),
        th.Property("responseStartedAt", th.DateTimeType, description="When the contact started the survey"),
        th.Property("responseCompletedAt", th.DateTimeType, description="When the contact finished the survey"),
        th.Property("surveySessionId", th.StringType, description="Survey session ID (FS_...), matches in-progress responseId"),
        th.Property("responseId", th.StringType, description="Completed response ID (R_...)"),
        th.Property("surveyLink", th.StringType, description="The contact's personal survey link; treat as a credential"),
    ).to_dict()

    # Responses that mean "this distribution has no retrievable history", not an
    # error with the sync: sent distributions whose history permanently 404s with
    # CP_GW_LD_NOT_FOUND, and TestEmail distributions, which return 400
    # EMAIL_DISTRIBUTION_NO_RECIPIENTS ("This distribution type may be unsupported").
    # Anything else (auth, rate limits, other 4xx/5xx) still fails the stream.
    NO_HISTORY_400_CODES = frozenset({"EMAIL_DISTRIBUTION_NO_RECIPIENTS"})

    def _no_history_reason(self, response: requests.Response) -> str | None:
        if response.status_code not in (400, 404):
            return None
        try:
            code = response.json().get("meta", {}).get("error", {}).get("errorCode")
        except ValueError:
            code = None
        if response.status_code == 404 or code in self.NO_HISTORY_400_CODES:
            return f"{response.status_code} {code}"
        return None

    def validate_response(self, response: requests.Response) -> None:
        if self._no_history_reason(response):
            return
        super().validate_response(response)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        reason = self._no_history_reason(response)
        if reason:
            self.logger.warning(
                "No history available for %s (%s); skipping",
                response.request.path_url,
                reason,
            )
            return
        yield from super().parse_response(response)
