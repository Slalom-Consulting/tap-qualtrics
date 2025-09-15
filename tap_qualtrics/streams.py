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

from singer_sdk import typing as th  # JSON Schema typing helpers

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