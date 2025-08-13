"""Stream type classes for tap-qualtrics."""

from __future__ import annotations

import time
import typing as t
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

# Custom type for fields that can be either string or integer
StringOrIntegerType = th.CustomType({
    "type": ["string", "integer"]
})

# Custom type for fields that can be either boolean or string representation of boolean
BooleanOrStringType = th.CustomType({
    "type": ["boolean", "string"]
})


class SurveyResponsesStream(QualtricsStream):
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(__name__)
    
    @property
    def path(self) -> str:
        # partition? how do we get differnt survey_ids
        return f"/API/v3/surveys/{self.config['survey_id']}/export-responses"
    
    
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

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        payload: dict = {}
        payload['sortByLastModifiedDate'] = "true"
        # payload['startDate'] = '2025-07-01T00:00:00Z'  # Example start date
        starting_date = self.get_starting_timestamp(context) or self.config.get('start_date')
        payload['startDate'] = starting_date.isoformat() if starting_date else None
        # payload['useLabels'] = "true"  # Example parameter to use labels
        payload['format'] = "ndjson"  # Example format parameter
        logging.info(f"Request payload: {payload}")

        return payload

    def parse_response(self, response:requests.Response) -> t.Iterable[dict]:
        """Parse the response from the API.

        Args:
            response: The HTTP response object.

        Returns:
            An iterable of records parsed from the response.
        """
        if response.status_code != 200:
            self.logger.error(f"Failed to create survey responses export file: {response.status_code}")
            return []

        data = response.json()
        records = data.get('responses', [])
        self.logger.info(f"Fetched {len(records)} records.")
        self.logger.info(f"Response data: {data}")
        progress_id = data.get('result', {}).get('progressId')
        if not progress_id:
            self.logger.error("No progress ID found in the response.")
            return []
    
        max_attempts = 3
        attempt = 0

        time.sleep(5)  # Initial wait before checking file readiness

        while attempt < max_attempts:
            attempt += 1
            self.logger.info(f"Attempt {attempt} to check if the file is ready.")

            file_id = self.is_file_ready(progress_id)
            if file_id:
                self.logger.info("File is ready for download.")
                break
            
            self.logger.info("File not ready yet, waiting before retrying...")
            time.sleep(30)
        else:
            self.logger.error("File not ready after maximum attempts.")
            return []

        # Now we can download the file
        self.logger.info("File is ready, proceeding to download.")
        responses_data = self.download_file(self.file_id)
        if not responses_data:
            self.logger.error("Failed to download the responses file.")
            return []
    
        self.logger.info(f"Downloaded and parsed responses data")
        
        # Extract the actual survey responses
        responses = responses_data.get('responses', [])
        self.logger.info(f"Found {len(responses)} survey responses")
        
        return responses

    def download_file(self, file_id: str) -> dict | None:
        """Download the file using the file ID."""
        url = f"{self.url_base}/API/v3/surveys/{self.config['survey_id']}/export-responses/{file_id}/file"
        headers = self.http_headers
        headers['x-api-token'] = self.config.get("api_token")
        
        try:
            self.logger.info(f"Downloading file from {url}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            # Write the raw zip file to disk
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            survey_id = self.config.get('survey_id', 'unknown')
            zip_filename = f"qualtrics_export_{survey_id}_{file_id}_{timestamp}.zip"
            
            with open(zip_filename, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Downloaded zip file saved as: {zip_filename}")
            
            # The response is a zip file containing ndjson
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
                
                # Look for JSON file (usually the first or only file)
                json_file = None
                for filename in file_list:
                    if filename.endswith('.json') or filename.endswith('.ndjson'):
                        json_file = filename
                        break

                if not json_file and file_list:
                    # If no .json extension found, try the first file
                    json_file = file_list[0]
                
                if json_file:
                    with zip_file.open(json_file) as f:
                        ndjson_content = f.read().decode('utf-8')
                        
                        # Save the extracted ndjson content to disk
                        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                        survey_id = self.config.get('survey_id', 'unknown')
                        ndjson_filename = f"qualtrics_responses_{survey_id}_{timestamp}.ndjson"
                        
                        with open(ndjson_filename, 'w', encoding='utf-8') as ndjson_file:
                            ndjson_file.write(ndjson_content)
                        
                        self.logger.info(f"Extracted ndjson file saved as: {ndjson_filename}")
                        
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
            
            # Split by lines and parse each JSON object
            for line in ndjson_content.strip().split('\n'):
                if line.strip():  # Skip empty lines
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

    def is_file_ready(self, progress_id: str) -> bool:
        """Check if the file is ready for download."""
        # This could involve making another API call to check the status
        url = f"{self.url_base}/API/v3/surveys/{self.config['survey_id']}/export-responses/{progress_id}"
        self.logger.info(f"Checking if file is ready for progress ID: {progress_id}")

        headers = self.http_headers
        headers['x-api-token'] = self.config.get("api_token")
        try: 
            logging.info(f"Requesting file readiness from {url} with headers {headers}")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            status = data.get('result', {}).get('status')
            self.logger.info(f"File status: {status}")
            if status == 'complete':
                self.file_id = data.get('result', {}).get('fileId')
                return True 
        except requests.RequestException as e:
            self.logger.error(f"Error checking file readiness: {e}")
            return False
    
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
        return row

    name = "survey_responses"
    primary_keys = ["responseId"]
    replication_key = "last_modified_date"
    rest_method = "POST"
    records_jsonpath = "$[*]"

    schema = th.PropertiesList(
        # Top-level response identifier
        th.Property("responseId", th.StringType, description="Unique identifier for the survey response"),
        th.Property("last_modified_date", th.DateTimeType, description="Last modified date of the response"),
        
        # Values object - Core response metadata
        th.Property("values", th.ObjectType(
            th.Property("startDate", th.DateTimeType, description="Date and time when the response was started"),
            th.Property("endDate", th.DateTimeType, description="Date and time when the response was completed"),
            th.Property("status", th.IntegerType, description="Response status code"),
            th.Property("ipAddress", th.StringType, description="IP address of the respondent"),
            th.Property("progress", th.IntegerType, description="Completion progress percentage"),
            th.Property("duration", th.IntegerType, description="Duration in seconds to complete the survey"),
            th.Property("finished", th.IntegerType, description="Whether the response was finished (0/1)"),
            th.Property("recordedDate", th.DateTimeType, description="Date and time when the response was recorded"),
            th.Property("_recordId", th.StringType, description="Internal record identifier"),
            th.Property("locationLatitude", th.StringType, description="Geographic latitude of respondent"),
            th.Property("locationLongitude", th.StringType, description="Geographic longitude of respondent"),
            th.Property("_lastModifiedDate", th.DateTimeType, description="Last modification date"),
            
            # Survey questions - Likert scale responses (QID3, QID4_*, QID5_*, QID6_*)
            th.Property("QID3", th.IntegerType, description="Overall experience rating"),
            # QID4 series - Agreement scale questions (1-5)
            th.Property("QID4_1", th.IntegerType, description="Agreement scale response 1"),
            th.Property("QID4_2", th.IntegerType, description="Agreement scale response 2"),
            th.Property("QID4_3", th.IntegerType, description="Agreement scale response 3"),
            th.Property("QID4_4", th.IntegerType, description="Agreement scale response 4"),
            th.Property("QID4_5", th.IntegerType, description="Agreement scale response 5"),
            th.Property("QID4_6", th.IntegerType, description="Agreement scale response 6"),
            th.Property("QID4_7", th.IntegerType, description="Agreement scale response 7"),
            # QID5 series - Agreement scale questions (1-5)
            th.Property("QID5_1", th.IntegerType, description="Agreement scale response 1"),
            th.Property("QID5_2", th.IntegerType, description="Agreement scale response 2"),
            th.Property("QID5_3", th.IntegerType, description="Agreement scale response 3"),
            th.Property("QID5_4", th.IntegerType, description="Agreement scale response 4"),
            th.Property("QID5_5", th.IntegerType, description="Agreement scale response 5"),
            th.Property("QID5_6", th.IntegerType, description="Agreement scale response 6"),
            th.Property("QID5_7", th.IntegerType, description="Agreement scale response 7"),
            # QID6 series - Rating scale questions (6-9)
            th.Property("QID6_1", th.IntegerType, description="Rating scale response 1"),
            th.Property("QID6_2", th.IntegerType, description="Rating scale response 2"),
            th.Property("QID6_3", th.IntegerType, description="Rating scale response 3"),
            th.Property("QID6_4", th.IntegerType, description="Rating scale response 4"),
            th.Property("QID6_5", th.IntegerType, description="Rating scale response 5"),
            th.Property("QID6_6", th.IntegerType, description="Rating scale response 6"),
            th.Property("QID6_7", th.IntegerType, description="Rating scale response 7"),
            th.Property("QID6_8", th.IntegerType, description="Rating scale response 8"),
            th.Property("QID6_9", th.IntegerType, description="Rating scale response 9"),
            th.Property("QID6_10", th.IntegerType, description="Rating scale response 10"),
            
            # Text responses
            th.Property("QID23_1", th.StringType, description="Nomination/recognition text 1"),
            th.Property("QID17_TEXT", th.StringType, description="Open-ended text response for QID17"),
            th.Property("QID17_FollowUpPrompt", th.StringType, description="Follow-up prompt for QID17"),
            th.Property("QID26_1", th.StringType, description="Nomination/recognition text 2"),
            th.Property("QID18_TEXT", th.StringType, description="Open-ended text response for QID18"),
            th.Property("QID18_FollowUpPrompt", th.StringType, description="Follow-up prompt for QID18"),
            th.Property("QID13_TEXT", th.StringType, description="Open-ended text response for QID13"),
            th.Property("QID14_TEXT", th.StringType, description="Open-ended text response for QID14"),
            
            # Additional question responses
            th.Property("QID15", th.IntegerType, description="Binary choice response (0/1)"),
            
            # Derived Likert scores
            th.Property("SupportiveLeadershipLikert", th.IntegerType, description="Supportive leadership derived score"),
            th.Property("RecognitionLikert", th.IntegerType, description="Recognition derived score"),
            th.Property("OrganizationalGoalsLikert", th.IntegerType, description="Organizational goals derived score"),
            th.Property("InnovationLikert", th.IntegerType, description="Innovation derived score"),
            th.Property("EquityLikert", th.IntegerType, description="Equity derived score"),
            th.Property("CollaborationLikert", th.IntegerType, description="Collaboration derived score"),
            th.Property("ClearCommsLikert", th.IntegerType, description="Clear communications derived score"),
            th.Property("WellBeingLikert", th.IntegerType, description="Well-being derived score"),
            th.Property("PsychologicalSafetyLikert", th.IntegerType, description="Psychological safety derived score"),
            th.Property("MeaningfulWorkLikert", th.IntegerType, description="Meaningful work derived score"),
            th.Property("LoveOfWorkLikert", th.IntegerType, description="Love of work derived score"),
            th.Property("GrowthLikert", th.IntegerType, description="Growth derived score"),
            th.Property("FeedbackLikert", th.IntegerType, description="Feedback derived score"),
            th.Property("AutonomyLikert", th.IntegerType, description="Autonomy derived score"),
            
            # Original responses for AI analysis
            th.Property("QID17_OriginalResponse", th.StringType, description="Original response before AI processing"),
            th.Property("QID17_AICategory", th.StringType, description="AI categorization of QID17 response"),
            th.Property("QID18_OriginalResponse", th.StringType, description="Original response before AI processing"),
            th.Property("QID18_AICategory", th.StringType, description="AI categorization of QID18 response"),
            
            # Employee demographics and data
            th.Property("Next Anniversary", th.StringType, description="Next anniversary date"),
            th.Property("Hire Date", th.StringType, description="Employee hire date"),
            th.Property("Anniversary Type", th.StringType, description="Type of anniversary"),
            th.Property("Anniversary Test", th.StringType, description="Anniversary test field"),
            th.Property("Job Level", th.StringType, description="Employee job level"),
            th.Property("Survey Month", th.StringType, description="Month when survey was taken"),
            th.Property("Market", th.StringType, description="Employee market/location"),
            th.Property("Job Level Group", th.StringType, description="Job level grouping"),
            th.Property("Generation", th.StringType, description="Generational cohort"),
            th.Property("Race-Ethnicity", th.StringType, description="Race/ethnicity information"),
            th.Property("Job Family", th.StringType, description="Job family classification"),
            th.Property("Number of Direct Reports (Employees)", th.StringType, description="Number of direct reports"),
            th.Property("Organization", th.StringType, description="Organization unit"),
            th.Property("Management Chain - Level 05", th.StringType, description="Management chain level 5"),
            th.Property("Sex", th.StringType, description="Gender information"),
            th.Property("Month Number of Hire Date", th.StringType, description="Hire month number"),
            th.Property("Function", th.StringType, description="Employee function"),
            th.Property("Job Family Group", th.StringType, description="Job family group"),
            th.Property("Effective Date", th.StringType, description="Effective date"),
            th.Property("Management Chain - Level 04", th.StringType, description="Management chain level 4"),
            th.Property("Management Chain - Level 03", th.StringType, description="Management chain level 3"),
            th.Property("Next Anniversary in Years", th.StringType, description="Years until next anniversary"),
            th.Property("Length of Service in Months from Hire Date", th.StringType, description="Service length in months"),
            th.Property("GM", th.StringType, description="General Manager"),
            th.Property("Title", th.StringType, description="Employee title"),
            th.Property("Location", th.StringType, description="Employee location"),
            th.Property("Email", th.StringType, description="Employee email"),
            th.Property("Name", th.StringType, description="Employee name"),
            th.Property("UniqueIdentifier", th.StringType, description="Unique employee identifier"),
            
            # AI-generated sentiment analysis fields
            # QID13_TEXT AI analysis
            th.Property("QID13_TEXT_f00715df_939rt99f49pwActionability", th.StringType, description="AI actionability assessment"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEffort", th.StringType, description="AI effort assessment"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEffortNumeric", th.IntegerType, description="AI effort numeric score"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEmotIntensity", th.StringType, description="AI emotion intensity"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEmotion", th.ArrayType(th.StringType), description="AI detected emotions"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSenPol", th.IntegerType, description="AI sentiment polarity"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSenScore", th.IntegerType, description="AI sentiment score (-2 to +2)"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSentiment", th.StringType, description="AI sentiment classification"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopics", th.ArrayType(th.StringType), description="AI identified topics"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicSenLabel", th.ArrayType(th.StringType), description="AI topic sentiment labels"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicSenScore", th.ArrayType(th.StringType), description="AI topic sentiment scores"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicHierarchy1", th.ArrayType(th.StringType), description="AI topic hierarchy level 1"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicHierarchy2", th.ArrayType(th.StringType), description="AI topic hierarchy level 2"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwParTopics", th.ArrayType(th.StringType), description="AI parent topics"),
            
            # QID14_TEXT AI analysis
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tActionability", th.StringType, description="AI actionability assessment"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEffort", th.StringType, description="AI effort assessment"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEffortNumeric", th.IntegerType, description="AI effort numeric score"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEmotIntensity", th.StringType, description="AI emotion intensity"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEmotion", th.ArrayType(th.StringType), description="AI detected emotions"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSenPol", th.IntegerType, description="AI sentiment polarity"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSenScore", th.IntegerType, description="AI sentiment score (-2 to +2)"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSentiment", th.StringType, description="AI sentiment classification"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopics", th.ArrayType(th.StringType), description="AI identified topics"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicSenLabel", th.ArrayType(th.StringType), description="AI topic sentiment labels"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicSenScore", th.ArrayType(th.StringType), description="AI topic sentiment scores"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicHierarchy1", th.ArrayType(th.StringType), description="AI topic hierarchy level 1"),
            
            # Derived fields
            th.Property("QID4_1_DERIVED6vm3fx4", th.StringType, description="Derived field from QID4_1"),
            
            # Additional geographic fields (some responses have these)
            th.Property("Geography", th.StringType, description="Geographic region"),
            th.Property("Geography Region-US", th.StringType, description="US geography region"),
            th.Property("Metro", th.StringType, description="Metropolitan area"),
            th.Property("Office Affiliation", th.StringType, description="Office affiliation"),
            th.Property("Primary Role", th.StringType, description="Primary role"),
            th.Property("Legal Entity", th.StringType, description="Legal entity"),
            th.Property("Nearest Slalom Location", th.StringType, description="Nearest Slalom location"),
            th.Property("Workforce Taxonomy L1", th.StringType, description="Workforce taxonomy level 1"),
            th.Property("Workforce Taxonomy L2", th.StringType, description="Workforce taxonomy level 2"),
            th.Property("Workforce Taxonomy L3", th.StringType, description="Workforce taxonomy level 3"),
            th.Property("Workforce Taxonomy L4", th.StringType, description="Workforce taxonomy level 4"),
            th.Property("Workforce Taxonomy Group", th.StringType, description="Workforce taxonomy group"),
            th.Property("Geography_DERIVEDkt21lz6", th.StringType, description="Derived geography field"),
        )),
        
        # Labels object - Human-readable labels for responses
        th.Property("labels", th.ObjectType(
            th.Property("status", th.StringType, description="Status label"),
            th.Property("finished", th.StringType, description="Finished label"),
            th.Property("QID3", th.StringType, description="QID3 response label"),
            th.Property("QID4_1", th.StringType, description="QID4_1 response label"),
            th.Property("QID4_2", th.StringType, description="QID4_2 response label"),
            th.Property("QID4_3", th.StringType, description="QID4_3 response label"),
            th.Property("QID4_4", th.StringType, description="QID4_4 response label"),
            th.Property("QID4_5", th.StringType, description="QID4_5 response label"),
            th.Property("QID4_6", th.StringType, description="QID4_6 response label"),
            th.Property("QID4_7", th.StringType, description="QID4_7 response label"),
            th.Property("QID5_1", th.StringType, description="QID5_1 response label"),
            th.Property("QID5_2", th.StringType, description="QID5_2 response label"),
            th.Property("QID5_3", th.StringType, description="QID5_3 response label"),
            th.Property("QID5_4", th.StringType, description="QID5_4 response label"),
            th.Property("QID5_5", th.StringType, description="QID5_5 response label"),
            th.Property("QID5_6", th.StringType, description="QID5_6 response label"),
            th.Property("QID5_7", th.StringType, description="QID5_7 response label"),
            th.Property("QID6_1", th.StringType, description="QID6_1 response label"),
            th.Property("QID6_2", th.StringType, description="QID6_2 response label"),
            th.Property("QID6_3", th.StringType, description="QID6_3 response label"),
            th.Property("QID6_4", th.StringType, description="QID6_4 response label"),
            th.Property("QID6_5", th.StringType, description="QID6_5 response label"),
            th.Property("QID6_6", th.StringType, description="QID6_6 response label"),
            th.Property("QID6_7", th.StringType, description="QID6_7 response label"),
            th.Property("QID6_8", th.StringType, description="QID6_8 response label"),
            th.Property("QID6_9", th.StringType, description="QID6_9 response label"),
            th.Property("QID6_10", th.StringType, description="QID6_10 response label"),
            th.Property("QID15", th.StringType, description="QID15 response label"),
        )),
        
        # Survey display metadata
        th.Property("displayedFields", th.ArrayType(th.StringType), description="Fields that were displayed to the respondent"),
        th.Property("displayedValues", th.ObjectType(
            th.Property("QID15", th.ArrayType(th.IntegerType), description="Possible values for QID15"),
            th.Property("QID4_1_DERIVED6vm3fx4", th.ArrayType(th.StringType), description="Possible values for derived QID4_1"),
            th.Property("QID6_9", th.ArrayType(th.IntegerType), description="Possible values for QID6_9"),
            th.Property("QID4_7", th.ArrayType(th.IntegerType), description="Possible values for QID4_7"),
            th.Property("QID5_6", th.ArrayType(th.IntegerType), description="Possible values for QID5_6"),
            th.Property("QID6_5", th.ArrayType(th.IntegerType), description="Possible values for QID6_5"),
            th.Property("QID5_7", th.ArrayType(th.IntegerType), description="Possible values for QID5_7"),
            th.Property("QID6_6", th.ArrayType(th.IntegerType), description="Possible values for QID6_6"),
            th.Property("QID6_7", th.ArrayType(th.IntegerType), description="Possible values for QID6_7"),
            th.Property("QID6_8", th.ArrayType(th.IntegerType), description="Possible values for QID6_8"),
            th.Property("QID4_3", th.ArrayType(th.IntegerType), description="Possible values for QID4_3"),
            th.Property("QID5_2", th.ArrayType(th.IntegerType), description="Possible values for QID5_2"),
            th.Property("QID6_1", th.ArrayType(th.IntegerType), description="Possible values for QID6_1"),
            th.Property("QID4_4", th.ArrayType(th.IntegerType), description="Possible values for QID4_4"),
            th.Property("QID5_3", th.ArrayType(th.IntegerType), description="Possible values for QID5_3"),
            th.Property("QID6_2", th.ArrayType(th.IntegerType), description="Possible values for QID6_2"),
            th.Property("QID4_5", th.ArrayType(th.IntegerType), description="Possible values for QID4_5"),
            th.Property("QID3", th.ArrayType(th.IntegerType), description="Possible values for QID3"),
            th.Property("QID5_4", th.ArrayType(th.IntegerType), description="Possible values for QID5_4"),
            th.Property("QID6_3", th.ArrayType(th.IntegerType), description="Possible values for QID6_3"),
            th.Property("QID4_6", th.ArrayType(th.IntegerType), description="Possible values for QID4_6"),
            th.Property("QID5_5", th.ArrayType(th.IntegerType), description="Possible values for QID5_5"),
            th.Property("QID6_4", th.ArrayType(th.IntegerType), description="Possible values for QID6_4"),
            th.Property("QID4_1", th.ArrayType(th.IntegerType), description="Possible values for QID4_1"),
            th.Property("QID4_2", th.ArrayType(th.IntegerType), description="Possible values for QID4_2"),
            th.Property("QID5_1", th.ArrayType(th.IntegerType), description="Possible values for QID5_1"),
            th.Property("QID6_10", th.ArrayType(th.IntegerType), description="Possible values for QID6_10"),
        ), description="Possible values for displayed fields"),
    ).to_dict()

class SurveyResponsesInProgressStream(SurveyResponsesStream):
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(__name__)
    
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
        starting_date = self.config.get('start_date')
        payload['startDate'] = starting_date.isoformat() if starting_date else None
        payload['format'] = "ndjson"  # Example format parameter
        logging.info(f"Request payload: {payload}")

        return payload
                
    name = "survey_responses_in_progress"
    primary_keys = ["responseId"]
    rest_method = "POST"
    records_jsonpath = "$[*]"
    schema = th.PropertiesList(
        # Top-level response identifier
        th.Property("responseId", th.StringType, description="Unique identifier for the survey response"),
        
        # Values object - Core response metadata
        th.Property("values", th.ObjectType(
            th.Property("startDate", th.DateTimeType, description="Date and time when the response was started"),
            th.Property("endDate", th.DateTimeType, description="Date and time when the response was completed"),
            th.Property("status", th.IntegerType, description="Response status code"),
            th.Property("ipAddress", th.StringType, description="IP address of the respondent"),
            th.Property("progress", th.IntegerType, description="Completion progress percentage"),
            th.Property("duration", th.IntegerType, description="Duration in seconds to complete the survey"),
            th.Property("finished", th.IntegerType, description="Whether the response was finished (0/1)"),
            th.Property("recordedDate", th.DateTimeType, description="Date and time when the response was recorded"),
            th.Property("_recordId", th.StringType, description="Internal record identifier"),
            th.Property("locationLatitude", th.StringType, description="Geographic latitude of respondent"),
            th.Property("locationLongitude", th.StringType, description="Geographic longitude of respondent"),
            th.Property("_lastModifiedDate", th.DateTimeType, description="Last modification date"),
            
            # Survey questions - Likert scale responses (QID3, QID4_*, QID5_*, QID6_*)
            th.Property("QID3", th.IntegerType, description="Overall experience rating"),
            # QID4 series - Agreement scale questions (1-5)
            th.Property("QID4_1", th.IntegerType, description="Agreement scale response 1"),
            th.Property("QID4_2", th.IntegerType, description="Agreement scale response 2"),
            th.Property("QID4_3", th.IntegerType, description="Agreement scale response 3"),
            th.Property("QID4_4", th.IntegerType, description="Agreement scale response 4"),
            th.Property("QID4_5", th.IntegerType, description="Agreement scale response 5"),
            th.Property("QID4_6", th.IntegerType, description="Agreement scale response 6"),
            th.Property("QID4_7", th.IntegerType, description="Agreement scale response 7"),
            # QID5 series - Agreement scale questions (1-5)
            th.Property("QID5_1", th.IntegerType, description="Agreement scale response 1"),
            th.Property("QID5_2", th.IntegerType, description="Agreement scale response 2"),
            th.Property("QID5_3", th.IntegerType, description="Agreement scale response 3"),
            th.Property("QID5_4", th.IntegerType, description="Agreement scale response 4"),
            th.Property("QID5_5", th.IntegerType, description="Agreement scale response 5"),
            th.Property("QID5_6", th.IntegerType, description="Agreement scale response 6"),
            th.Property("QID5_7", th.IntegerType, description="Agreement scale response 7"),
            # QID6 series - Rating scale questions (6-9)
            th.Property("QID6_1", th.IntegerType, description="Rating scale response 1"),
            th.Property("QID6_2", th.IntegerType, description="Rating scale response 2"),
            th.Property("QID6_3", th.IntegerType, description="Rating scale response 3"),
            th.Property("QID6_4", th.IntegerType, description="Rating scale response 4"),
            th.Property("QID6_5", th.IntegerType, description="Rating scale response 5"),
            th.Property("QID6_6", th.IntegerType, description="Rating scale response 6"),
            th.Property("QID6_7", th.IntegerType, description="Rating scale response 7"),
            th.Property("QID6_8", th.IntegerType, description="Rating scale response 8"),
            th.Property("QID6_9", th.IntegerType, description="Rating scale response 9"),
            th.Property("QID6_10", th.IntegerType, description="Rating scale response 10"),
            
            # Text responses
            th.Property("QID23_1", th.StringType, description="Nomination/recognition text 1"),
            th.Property("QID17_TEXT", th.StringType, description="Open-ended text response for QID17"),
            th.Property("QID17_FollowUpPrompt", th.StringType, description="Follow-up prompt for QID17"),
            th.Property("QID26_1", th.StringType, description="Nomination/recognition text 2"),
            th.Property("QID18_TEXT", th.StringType, description="Open-ended text response for QID18"),
            th.Property("QID18_FollowUpPrompt", th.StringType, description="Follow-up prompt for QID18"),
            th.Property("QID13_TEXT", th.StringType, description="Open-ended text response for QID13"),
            th.Property("QID14_TEXT", th.StringType, description="Open-ended text response for QID14"),
            
            # Additional question responses
            th.Property("QID15", th.IntegerType, description="Binary choice response (0/1)"),
            
            # Derived Likert scores
            th.Property("SupportiveLeadershipLikert", th.IntegerType, description="Supportive leadership derived score"),
            th.Property("RecognitionLikert", th.IntegerType, description="Recognition derived score"),
            th.Property("OrganizationalGoalsLikert", th.IntegerType, description="Organizational goals derived score"),
            th.Property("InnovationLikert", th.IntegerType, description="Innovation derived score"),
            th.Property("EquityLikert", th.IntegerType, description="Equity derived score"),
            th.Property("CollaborationLikert", th.IntegerType, description="Collaboration derived score"),
            th.Property("ClearCommsLikert", th.IntegerType, description="Clear communications derived score"),
            th.Property("WellBeingLikert", th.IntegerType, description="Well-being derived score"),
            th.Property("PsychologicalSafetyLikert", th.IntegerType, description="Psychological safety derived score"),
            th.Property("MeaningfulWorkLikert", th.IntegerType, description="Meaningful work derived score"),
            th.Property("LoveOfWorkLikert", th.IntegerType, description="Love of work derived score"),
            th.Property("GrowthLikert", th.IntegerType, description="Growth derived score"),
            th.Property("FeedbackLikert", th.IntegerType, description="Feedback derived score"),
            th.Property("AutonomyLikert", th.IntegerType, description="Autonomy derived score"),
            
            # Original responses for AI analysis
            th.Property("QID17_OriginalResponse", th.StringType, description="Original response before AI processing"),
            th.Property("QID17_AICategory", th.StringType, description="AI categorization of QID17 response"),
            th.Property("QID18_OriginalResponse", th.StringType, description="Original response before AI processing"),
            th.Property("QID18_AICategory", th.StringType, description="AI categorization of QID18 response"),
            
            # Employee demographics and data
            th.Property("Next Anniversary", th.StringType, description="Next anniversary date"),
            th.Property("Hire Date", th.StringType, description="Employee hire date"),
            th.Property("Anniversary Type", th.StringType, description="Type of anniversary"),
            th.Property("Anniversary Test", th.StringType, description="Anniversary test field"),
            th.Property("Job Level", th.StringType, description="Employee job level"),
            th.Property("Survey Month", th.StringType, description="Month when survey was taken"),
            th.Property("Market", th.StringType, description="Employee market/location"),
            th.Property("Job Level Group", th.StringType, description="Job level grouping"),
            th.Property("Generation", th.StringType, description="Generational cohort"),
            th.Property("Race-Ethnicity", th.StringType, description="Race/ethnicity information"),
            th.Property("Job Family", th.StringType, description="Job family classification"),
            th.Property("Number of Direct Reports (Employees)", th.StringType, description="Number of direct reports"),
            th.Property("Organization", th.StringType, description="Organization unit"),
            th.Property("Management Chain - Level 05", th.StringType, description="Management chain level 5"),
            th.Property("Sex", th.StringType, description="Gender information"),
            th.Property("Month Number of Hire Date", th.StringType, description="Hire month number"),
            th.Property("Function", th.StringType, description="Employee function"),
            th.Property("Job Family Group", th.StringType, description="Job family group"),
            th.Property("Effective Date", th.StringType, description="Effective date"),
            th.Property("Management Chain - Level 04", th.StringType, description="Management chain level 4"),
            th.Property("Management Chain - Level 03", th.StringType, description="Management chain level 3"),
            th.Property("Next Anniversary in Years", th.StringType, description="Years until next anniversary"),
            th.Property("Length of Service in Months from Hire Date", th.StringType, description="Service length in months"),
            th.Property("GM", th.StringType, description="General Manager"),
            th.Property("Title", th.StringType, description="Employee title"),
            th.Property("Location", th.StringType, description="Employee location"),
            th.Property("Email", th.StringType, description="Employee email"),
            th.Property("Name", th.StringType, description="Employee name"),
            th.Property("UniqueIdentifier", th.StringType, description="Unique employee identifier"),
            
            # AI-generated sentiment analysis fields
            # QID13_TEXT AI analysis
            th.Property("QID13_TEXT_f00715df_939rt99f49pwActionability", th.StringType, description="AI actionability assessment"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEffort", th.StringType, description="AI effort assessment"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEffortNumeric", th.IntegerType, description="AI effort numeric score"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEmotIntensity", th.StringType, description="AI emotion intensity"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwEmotion", th.ArrayType(th.StringType), description="AI detected emotions"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSenPol", th.IntegerType, description="AI sentiment polarity"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSenScore", th.IntegerType, description="AI sentiment score (-2 to +2)"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwSentiment", th.StringType, description="AI sentiment classification"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopics", th.ArrayType(th.StringType), description="AI identified topics"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicSenLabel", th.ArrayType(th.StringType), description="AI topic sentiment labels"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicSenScore", th.ArrayType(th.StringType), description="AI topic sentiment scores"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicHierarchy1", th.ArrayType(th.StringType), description="AI topic hierarchy level 1"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwTopicHierarchy2", th.ArrayType(th.StringType), description="AI topic hierarchy level 2"),
            th.Property("QID13_TEXT_f00715df_939rt99f49pwParTopics", th.ArrayType(th.StringType), description="AI parent topics"),
            
            # QID14_TEXT AI analysis
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tActionability", th.StringType, description="AI actionability assessment"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEffort", th.StringType, description="AI effort assessment"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEffortNumeric", th.IntegerType, description="AI effort numeric score"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEmotIntensity", th.StringType, description="AI emotion intensity"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tEmotion", th.ArrayType(th.StringType), description="AI detected emotions"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSenPol", th.IntegerType, description="AI sentiment polarity"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSenScore", th.IntegerType, description="AI sentiment score (-2 to +2)"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tSentiment", th.StringType, description="AI sentiment classification"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopics", th.ArrayType(th.StringType), description="AI identified topics"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicSenLabel", th.ArrayType(th.StringType), description="AI topic sentiment labels"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicSenScore", th.ArrayType(th.StringType), description="AI topic sentiment scores"),
            th.Property("QID14_TEXT_f00715df_4nw7bx1zd48tTopicHierarchy1", th.ArrayType(th.StringType), description="AI topic hierarchy level 1"),
            
            # Derived fields
            th.Property("QID4_1_DERIVED6vm3fx4", th.StringType, description="Derived field from QID4_1"),
            
            # Additional geographic fields (some responses have these)
            th.Property("Geography", th.StringType, description="Geographic region"),
            th.Property("Geography Region-US", th.StringType, description="US geography region"),
            th.Property("Metro", th.StringType, description="Metropolitan area"),
            th.Property("Office Affiliation", th.StringType, description="Office affiliation"),
            th.Property("Primary Role", th.StringType, description="Primary role"),
            th.Property("Legal Entity", th.StringType, description="Legal entity"),
            th.Property("Nearest Slalom Location", th.StringType, description="Nearest Slalom location"),
            th.Property("Workforce Taxonomy L1", th.StringType, description="Workforce taxonomy level 1"),
            th.Property("Workforce Taxonomy L2", th.StringType, description="Workforce taxonomy level 2"),
            th.Property("Workforce Taxonomy L3", th.StringType, description="Workforce taxonomy level 3"),
            th.Property("Workforce Taxonomy L4", th.StringType, description="Workforce taxonomy level 4"),
            th.Property("Workforce Taxonomy Group", th.StringType, description="Workforce taxonomy group"),
            th.Property("Geography_DERIVEDkt21lz6", th.StringType, description="Derived geography field"),
        )),
        
        # Labels object - Human-readable labels for responses
        th.Property("labels", th.ObjectType(
            th.Property("status", th.StringType, description="Status label"),
            th.Property("finished", th.StringType, description="Finished label"),
            th.Property("QID3", th.StringType, description="QID3 response label"),
            th.Property("QID4_1", th.StringType, description="QID4_1 response label"),
            th.Property("QID4_2", th.StringType, description="QID4_2 response label"),
            th.Property("QID4_3", th.StringType, description="QID4_3 response label"),
            th.Property("QID4_4", th.StringType, description="QID4_4 response label"),
            th.Property("QID4_5", th.StringType, description="QID4_5 response label"),
            th.Property("QID4_6", th.StringType, description="QID4_6 response label"),
            th.Property("QID4_7", th.StringType, description="QID4_7 response label"),
            th.Property("QID5_1", th.StringType, description="QID5_1 response label"),
            th.Property("QID5_2", th.StringType, description="QID5_2 response label"),
            th.Property("QID5_3", th.StringType, description="QID5_3 response label"),
            th.Property("QID5_4", th.StringType, description="QID5_4 response label"),
            th.Property("QID5_5", th.StringType, description="QID5_5 response label"),
            th.Property("QID5_6", th.StringType, description="QID5_6 response label"),
            th.Property("QID5_7", th.StringType, description="QID5_7 response label"),
            th.Property("QID6_1", th.StringType, description="QID6_1 response label"),
            th.Property("QID6_2", th.StringType, description="QID6_2 response label"),
            th.Property("QID6_3", th.StringType, description="QID6_3 response label"),
            th.Property("QID6_4", th.StringType, description="QID6_4 response label"),
            th.Property("QID6_5", th.StringType, description="QID6_5 response label"),
            th.Property("QID6_6", th.StringType, description="QID6_6 response label"),
            th.Property("QID6_7", th.StringType, description="QID6_7 response label"),
            th.Property("QID6_8", th.StringType, description="QID6_8 response label"),
            th.Property("QID6_9", th.StringType, description="QID6_9 response label"),
            th.Property("QID6_10", th.StringType, description="QID6_10 response label"),
            th.Property("QID15", th.StringType, description="QID15 response label"),
        )),
        
        # Survey display metadata
        th.Property("displayedFields", th.ArrayType(th.StringType), description="Fields that were displayed to the respondent"),
        th.Property("displayedValues", th.ObjectType(
            th.Property("QID15", th.ArrayType(th.IntegerType), description="Possible values for QID15"),
            th.Property("QID4_1_DERIVED6vm3fx4", th.ArrayType(th.StringType), description="Possible values for derived QID4_1"),
            th.Property("QID6_9", th.ArrayType(th.IntegerType), description="Possible values for QID6_9"),
            th.Property("QID4_7", th.ArrayType(th.IntegerType), description="Possible values for QID4_7"),
            th.Property("QID5_6", th.ArrayType(th.IntegerType), description="Possible values for QID5_6"),
            th.Property("QID6_5", th.ArrayType(th.IntegerType), description="Possible values for QID6_5"),
            th.Property("QID5_7", th.ArrayType(th.IntegerType), description="Possible values for QID5_7"),
            th.Property("QID6_6", th.ArrayType(th.IntegerType), description="Possible values for QID6_6"),
            th.Property("QID6_7", th.ArrayType(th.IntegerType), description="Possible values for QID6_7"),
            th.Property("QID6_8", th.ArrayType(th.IntegerType), description="Possible values for QID6_8"),
            th.Property("QID4_3", th.ArrayType(th.IntegerType), description="Possible values for QID4_3"),
            th.Property("QID5_2", th.ArrayType(th.IntegerType), description="Possible values for QID5_2"),
            th.Property("QID6_1", th.ArrayType(th.IntegerType), description="Possible values for QID6_1"),
            th.Property("QID4_4", th.ArrayType(th.IntegerType), description="Possible values for QID4_4"),
            th.Property("QID5_3", th.ArrayType(th.IntegerType), description="Possible values for QID5_3"),
            th.Property("QID6_2", th.ArrayType(th.IntegerType), description="Possible values for QID6_2"),
            th.Property("QID4_5", th.ArrayType(th.IntegerType), description="Possible values for QID4_5"),
            th.Property("QID3", th.ArrayType(th.IntegerType), description="Possible values for QID3"),
            th.Property("QID5_4", th.ArrayType(th.IntegerType), description="Possible values for QID5_4"),
            th.Property("QID6_3", th.ArrayType(th.IntegerType), description="Possible values for QID6_3"),
            th.Property("QID4_6", th.ArrayType(th.IntegerType), description="Possible values for QID4_6"),
            th.Property("QID5_5", th.ArrayType(th.IntegerType), description="Possible values for QID5_5"),
            th.Property("QID6_4", th.ArrayType(th.IntegerType), description="Possible values for QID6_4"),
            th.Property("QID4_1", th.ArrayType(th.IntegerType), description="Possible values for QID4_1"),
            th.Property("QID4_2", th.ArrayType(th.IntegerType), description="Possible values for QID4_2"),
            th.Property("QID5_1", th.ArrayType(th.IntegerType), description="Possible values for QID5_1"),
            th.Property("QID6_10", th.ArrayType(th.IntegerType), description="Possible values for QID6_10"),
        ), description="Possible values for displayed fields"),
    ).to_dict()

class SurveyQuestionsStream(QualtricsStream):
    """Stream for Qualtrics survey questions."""
    
    def __init__(self, tap):
        super().__init__(tap=tap)
        self.logger = logging.getLogger(__name__)
    
    @property
    def path(self) -> str:
        return f"/API/v3/survey-definitions/{self.config['survey_id']}/questions"
    

    name = "survey_questions"
    primary_keys = ["QuestionID"]
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
        th.Property("QuestionText_Unsafe", th.StringType, description="Alternative unsafe question text field"),
        th.Property("DataExportTag", th.StringType, description="Data export tag for the question"),
        th.Property("DefaultChoices", th.BooleanType, description="Whether to use default choices"),
        th.Property("DataVisibility", th.ObjectType(
            th.Property("Private", th.BooleanType, description="Private visibility setting"),
            th.Property("Hidden", th.BooleanType, description="Hidden visibility setting"),
        ), description="Data visibility settings"),
        
        # Question configuration
        th.Property("Validation", th.ObjectType(
            th.Property("Settings", th.ObjectType(
                th.Property("Type", th.StringType, description="Validation type (None, AITextAnalysis, etc.)"),
                th.Property("ForceResponse", th.StringType, description="Force response validation (OFF, ON)"),
                th.Property("ForceResponseType", th.StringType, description="Force response type (ON, OFF)"),
                th.Property("AITextAnalysis", th.ObjectType(
                    th.Property("Checks", th.ArrayType(th.ObjectType(
                        th.Property("Type", th.StringType, description="Check type (Partial, OverlyGeneralized, etc.)"),
                        th.Property("Messages", th.StringType, description="Messages setting"),
                    )), description="AI text analysis checks"),
                ), description="AI text analysis configuration"),
            ), description="Validation settings object"),
        ), description="Question validation settings"),
        th.Property("GradingData", th.ArrayType(th.ObjectType()), description="Grading configuration for the question"),
        th.Property("Language", th.ArrayType(th.ObjectType()), description="Language-specific question data"),
        th.Property("NextChoiceId", th.IntegerType, description="Next available choice ID"),
        th.Property("NextAnswerId", th.IntegerType, description="Next available answer ID"),
        
        # Question choices/answers - Updated based on actual data structure
        th.Property("Choices", th.ObjectType(
            # Dynamic choice properties based on actual data
            th.Property("1", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 1"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 1"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 1"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 1"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 1"),
            ), description="Choice 1"),
            th.Property("2", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 2"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 2"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 2"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 2"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 2"),
            ), description="Choice 2"),
            th.Property("3", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 3"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 3"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 3"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 3"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 3"),
            ), description="Choice 3"),
            th.Property("4", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 4"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 4"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 4"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 4"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 4"),
            ), description="Choice 4"),
            th.Property("5", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 5"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 5"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 5"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 5"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 5"),
            ), description="Choice 5"),
            th.Property("6", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 6"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 6"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 6"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 6"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 6"),
            ), description="Choice 6"),
            th.Property("7", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 7"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 7"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 7"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 7"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 7"),
            ), description="Choice 7"),
            th.Property("8", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 8"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 8"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 8"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 8"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 8"),
            ), description="Choice 8"),
            th.Property("9", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 9"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 9"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 9"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 9"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 9"),
            ), description="Choice 9"),
            th.Property("10", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for choice 10"),
                th.Property("TextEntryLength", StringOrIntegerType, description="Text entry length for choice 10"),
                th.Property("TextEntry", StringOrIntegerType, description="Text entry type for choice 10"),
                th.Property("InputHeight", th.IntegerType, description="Input height for choice 10"),
                th.Property("InputWidth", th.IntegerType, description="Input width for choice 10"),
            ), description="Choice 10"),
        ), description="Available choices for the question"),
        th.Property("ChoiceOrder", th.ArrayType(StringOrIntegerType), description="Order of choices"),
        th.Property("Answers", th.ObjectType(
            # Dynamic answer properties for matrix questions
            th.Property("6", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for answer 6"),
            ), description="Answer 6"),
            th.Property("7", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for answer 7"),
            ), description="Answer 7"),
            th.Property("8", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for answer 8"),
            ), description="Answer 8"),
            th.Property("9", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for answer 9"),
            ), description="Answer 9"),
            th.Property("10", th.ObjectType(
                th.Property("Display", th.StringType, description="Display text for answer 10"),
            ), description="Answer 10"),
        ), description="Available answers for matrix questions"),
        th.Property("AnswerOrder", th.ArrayType(StringOrIntegerType), description="Order of answers"),
        
        # Display and behavior settings
        th.Property("RecodeValues", th.ObjectType(
            # Dynamic recode properties
            th.Property("1", th.StringType, description="Recode value for choice 1"),
            th.Property("2", th.StringType, description="Recode value for choice 2"),
            th.Property("6", th.StringType, description="Recode value for choice 6"),
            th.Property("7", th.StringType, description="Recode value for choice 7"),
            th.Property("8", th.StringType, description="Recode value for choice 8"),
            th.Property("9", th.StringType, description="Recode value for choice 9"),
            th.Property("10", th.StringType, description="Recode value for choice 10"),
        ), description="Recode values for choices"),
        th.Property("ChoiceDataExportTags", th.BooleanType, description="Data export tags for choices"),
        th.Property("VariableNaming", th.ObjectType(), description="Variable naming configuration"),
        th.Property("ColumnSubQuestion", th.BooleanType, description="Whether this is a column sub-question"),
        
        # Question flow and logic
        th.Property("QuestionJS", BooleanOrStringType, description="JavaScript code for the question"),
        th.Property("DisplayLogic", th.ObjectType(), description="Display logic configuration"),
        th.Property("ChoiceRandomization", th.ObjectType(), description="Choice randomization settings"),
        
        # Additional configuration - Updated based on actual data structure
        th.Property("Configuration", th.ObjectType(
            th.Property("QuestionDescriptionOption", th.StringType, description="Question description option (UseText, SpecifyLabel)"),
            th.Property("NumColumns", th.IntegerType, description="Number of columns for display"),
            th.Property("InputWidth", th.IntegerType, description="Input field width"),
            th.Property("InputHeight", th.IntegerType, description="Input field height"),
            th.Property("TextPosition", th.StringType, description="Text position configuration"),
            th.Property("ChoiceColumnWidth", th.IntegerType, description="Choice column width"),
            th.Property("RepeatHeaders", th.StringType, description="Whether to repeat headers"),
            th.Property("WhiteSpace", th.StringType, description="White space configuration"),
            th.Property("MobileFirst", th.BooleanType, description="Mobile first configuration"),
            th.Property("ChoiceColumnWidthPixels", th.IntegerType, description="Choice column width in pixels"),
            th.Property("LabelPosition", th.StringType, description="Label position configuration"),
            th.Property("Stack", th.StringType, description="Stack configuration for question layout"),
            th.Property("SearchResultTemplate", th.ObjectType(
                th.Property("Type", th.StringType, description="Search result template type"),
                th.Property("DisplayFields", th.ObjectType(
                    th.Property("Primary", th.StringType, description="Primary display field"),
                ), description="Display fields configuration"),
            ), description="Search result template configuration"),
        ), description="Additional question configuration"),
        
        # Question metadata - Updated based on actual data structure
        th.Property("QuestionInstructionText", th.StringType, description="Instruction text for the question"),
        th.Property("SearchSource", th.ObjectType(
            th.Property("AllowFreeResponse", BooleanOrStringType, description="Allow free response in search (can be boolean or string)"),
            th.Property("Type", th.StringType, description="Search source type"),
            th.Property("SearchFields", th.ArrayType(th.StringType), description="Search fields"),
            th.Property("Limit", th.IntegerType, description="Search result limit"),
            th.Property("SourceID", th.StringType, description="Source ID"),
            th.Property("StoreField", th.StringType, description="Store field"),
        ), description="Search source configuration"),
        th.Property("DynamicChoices", th.ObjectType(), description="Dynamic choices configuration"),
        
        # Experimental or advanced features
        th.Property("AddOnProperties", th.ObjectType(), description="Add-on properties"),
        th.Property("AnalyzeChoices", th.ObjectType(), description="Analysis choices configuration"),
        
        # Question grouping and organization
        th.Property("Block", th.StringType, description="Block ID this question belongs to"),
        th.Property("QuestionNumber", th.IntegerType, description="Question number in survey"),
        
    ).to_dict()


