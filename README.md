# tap-qualtrics

`tap-qualtrics` is a Singer tap for Qualtrics that extracts survey data including responses, questions, and survey definitions.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Features

- **Multiple Stream Types**: Extract survey responses, in-progress responses, questions, and survey definitions
- **Async File Processing**: Handles Qualtrics' async export workflow with automatic polling
- **Survey Partitioning**: Processes multiple surveys in parallel using partitioned streams
- **Incremental Sync**: Supports incremental extraction based on last modified dates
- **Robust Error Handling**: Comprehensive retry logic with configurable timeouts

## Project Structure

### Architecture Overview

```
tap_qualtrics/
├── tap.py                    # Main tap class with configuration schema
├── client.py                 # Base QualtricsStream class with authentication
└── streams.py                # All stream implementations
```

### Stream Class Hierarchy

```
QualtricsStream (client.py)
├── SurveyResponsesStream
│   └── SurveyResponsesInProgressStream (inherits from SurveyResponsesStream)
├── SurveyQuestionsStream
└── SurveyDefinitionStream
```

**Base Class (`QualtricsStream`)**:
- Handles API authentication using API key headers
- Implements partitioning logic for multiple surveys
- Provides base URL and HTTP headers configuration
- Manages survey ID partitions from configuration

**Survey Response Streams**:
- `SurveyResponsesStream`: Exports completed survey responses with async file processing
- `SurveyResponsesInProgressStream`: Inherits from responses stream but exports in-progress responses

**Metadata Streams**:
- `SurveyQuestionsStream`: Extracts question definitions and metadata
- `SurveyDefinitionStream`: Extracts complete survey structure and configuration

### Stream Processing Flow

#### For Response Streams (Async Export Pattern):
1. **Initial Request**: POST to export endpoint with filters and format
2. **Progress Polling**: Wait for file generation using configurable retry logic
3. **File Download**: Download ZIP file containing NDJSON data
4. **Data Extraction**: Parse NDJSON and yield individual survey responses
5. **Post Processing**: Add survey_id and last_modified_date to each record

#### For Metadata Streams (Direct API Pattern):
1. **Direct Request**: GET request to survey definition endpoints
2. **JSON Parsing**: Extract data using JSONPath expressions
3. **Record Yielding**: Stream records directly from API response

### Partitioning Strategy

The tap uses **survey-based partitioning** to process multiple surveys efficiently:

**Partition Context**:
```python
[
    {"survey_id": "SV_abc123"},
    {"survey_id": "SV_def456"},
    {"survey_id": "SV_ghi789"}
]
```

**How It Works**:
- Each survey ID from `survey_ids` config becomes a separate partition
- Streams process each survey independently and in parallel
- State is maintained per partition for incremental sync
- URL templates use `{survey_id}` placeholder for dynamic endpoint generation

**State Management**:
- Incremental streams track `last_modified_date` per survey partition
- State allows resuming from last successful sync point per survey
- Failed surveys don't block processing of other surveys

## Configuration

## Configuration

### Required Settings

| Setting | Type | Description |
|---------|------|-------------|
| `api_token` | string | Qualtrics API token (required, secret) |
| `survey_ids` | array | List of survey IDs to extract data from (required) |
| `url_base` | string | Qualtrics base URL (default: "https://pdx1.qualtrics.com") |

### Optional Settings

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `start_date` | datetime | null | Earliest record date to sync (ISO format) |
| `max_file_ready_attempts` | string | "3" | Max attempts to check if export file is ready |
| `initial_wait_seconds` | string | "5" | Initial wait time before checking file status |
| `retry_wait_seconds` | string | "10" | Wait time between file status check retries |

### Example Configuration

```json
{
  "api_token": "your-qualtrics-api-token-here",
  "survey_ids": [
    "SV_1234567890abcdef",
    "SV_0987654321fedcba"
  ],
  "url_base": "https://yourdatacenter.qualtrics.com",
  "start_date": "2024-01-01T00:00:00Z",
  "max_file_ready_attempts": "5",
  "initial_wait_seconds": "10",
  "retry_wait_seconds": "15"
}
```

### Authentication Setup

1. **Generate API Token**:
   - Log into your Qualtrics account
   - Go to Account Settings → Qualtrics IDs
   - Generate a new API token
   - Copy the token (treat as sensitive data)

2. **Find Survey IDs**:
   - Navigate to your survey
   - Survey ID is in the URL: `https://yourinstance.qualtrics.com/Q/EditSection/Blocks/Ajax/SV_XXXXXXXXXXXXXXXXX`
   - Or use the Qualtrics API to list surveys

3. **Determine Data Center**:
   - Check your Qualtrics URL for the data center prefix
   - Common examples:
     - `https://pdx1.qualtrics.com` (Portland)
     - `https://ca1.qualtrics.com` (Canada)  
     - `https://eu.qualtrics.com` (Europe)

### Stream-Specific Behavior

**SurveyResponsesStream**:
- Uses `last_modified_date` as replication key for incremental sync
- Exports completed responses only
- Handles large datasets via async export workflow

**SurveyResponsesInProgressStream**:
- No replication key (full refresh only)
- Exports in-progress responses
- Useful for monitoring survey completion rates

**SurveyQuestionsStream & SurveyDefinitionStream**:
- Full refresh streams (metadata changes infrequently)
- Direct API calls (no async export needed)
- Adds `survey_id` field to all records for cross-referencing

## Available Streams

| Stream Name | Primary Key | Replication Method | Description |
|-------------|-------------|-------------------|-------------|
| `survey_responses` | `responseId` | Incremental (`last_modified_date`) | Completed survey responses |
| `survey_responses_in_progress` | `responseId` | Full Refresh | In-progress survey responses |
| `survey_questions` | `QuestionID` | Full Refresh | Question definitions and metadata |
| `survey_definition` | `SurveyID` | Full Refresh | Complete survey structure and settings |

### Data Schema Examples

**Survey Response Record**:
```json
{
  "responseId": "R_abc123",
  "last_modified_date": "2024-01-15T10:30:00Z",
  "survey_id": "SV_def456",
  "values": {
    "Q1": "Yes",
    "Q2_1": 5,
    "_lastModifiedDate": "2024-01-15T10:30:00Z"
  },
  "labels": {
    "Q1": "Yes", 
    "Q2_1": "Extremely Satisfied"
  },
  "displayedFields": ["Q1", "Q2"],
  "displayedValues": {...}
}
```

**Survey Question Record**:
```json
{
  "QuestionID": "QID1",
  "survey_id": "SV_def456",
  "QuestionType": "MC",
  "QuestionText": "How satisfied are you?",
  "Choices": {
    "1": {"Display": "Very Dissatisfied"},
    "2": {"Display": "Somewhat Dissatisfied"}
  },
  "ChoiceOrder": [1, 2, 3, 4, 5]
}
```

## Usage

### Basic Usage

```bash
# Discover available streams and fields
tap-qualtrics --config config.json --discover > catalog.json

# Run full extraction
tap-qualtrics --config config.json --catalog catalog.json

# Run with target (example with target-jsonl)
tap-qualtrics --config config.json --catalog catalog.json | target-jsonl
```

### Selecting Streams

Edit your `catalog.json` to select which streams to extract:

```json
{
  "streams": [
    {
      "tap_stream_id": "survey_responses",
      "schema": {...},
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "selected": true,
            "replication-method": "INCREMENTAL",
            "replication-key": "last_modified_date"
          }
        }
      ]
    }
  ]
}
```

### Environment Variables

Set configuration via environment variables:

```bash
export TAP_QUALTRICS_API_TOKEN="your-token"
export TAP_QUALTRICS_SURVEY_IDS='["SV_123", "SV_456"]'
export TAP_QUALTRICS_START_DATE="2024-01-01T00:00:00Z"

tap-qualtrics --config ENV --discover
```

### Incremental Sync

For incremental streams, the tap maintains state:

```json
{
  "bookmarks": {
    "survey_responses": {
      "partitions": [
        {
          "context": {"survey_id": "SV_123"},
          "replication_key_value": "2024-01-15T10:30:00Z"
        }
      ]
    }
  }
}
```

Resume from last sync:
```bash
tap-qualtrics --config config.json --catalog catalog.json --state state.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Prerequisites:

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-qualtrics` CLI interface directly using `uv run`:

```bash
uv run tap-qualtrics --help
```

### Testing with [Meltano](https://www.meltano.com)

Add to your `meltano.yml`:

```yaml
extractors:
  - name: tap-qualtrics
    pip_url: -e .
    settings:
      - name: api_token
        kind: password
      - name: survey_ids
        kind: array
      - name: url_base  
        default: "https://pdx1.qualtrics.com"
      - name: start_date
        kind: date_iso8601
      - name: max_file_ready_attempts
        default: "3"
      - name: initial_wait_seconds
        default: "5" 
      - name: retry_wait_seconds
        default: "10"
    select:
      - "survey_responses.*"
      - "survey_questions.*"

targets:
  - name: target-jsonl
    pip_url: target-jsonl
```

Run with Meltano:
```bash
# Test the tap
meltano invoke tap-qualtrics --discover

# Run ELT pipeline
meltano run tap-qualtrics target-jsonl

# Test single survey
meltano config tap-qualtrics set survey_ids '["SV_123"]'
meltano run tap-qualtrics target-jsonl
```

## Implementation Details

### Async File Processing Workflow

For survey responses, Qualtrics uses an async export pattern:

1. **Export Request**: POST to `/API/v3/surveys/{survey_id}/export-responses`
   - Returns `progressId` for tracking
   - File generation happens asynchronously

2. **Polling Loop**: GET `/API/v3/surveys/{survey_id}/export-responses/{progress_id}`
   - Configurable retry attempts and wait times
   - Continues until status becomes `"complete"`

3. **File Download**: GET `/API/v3/surveys/{survey_id}/export-responses/{file_id}/file`  
   - Downloads ZIP file containing NDJSON data
   - Handles large survey datasets efficiently

4. **Data Processing**:
   - Extracts NDJSON from ZIP archive
   - Parses line-delimited JSON records
   - Yields individual survey responses

### Error Handling & Resilience

- **Request Timeouts**: All HTTP requests include timeout handling
- **Retry Logic**: Configurable retry attempts for file readiness checking  
- **Graceful Degradation**: Failed surveys don't block other survey processing
- **Detailed Logging**: Comprehensive logging for debugging and monitoring
- **Input Validation**: Configuration validation with helpful error messages

### Performance Considerations

- **Partitioned Processing**: Each survey processes independently
- **Streaming Data**: Records are yielded one at a time (memory efficient)
- **Configurable Polling**: Adjust timing based on survey size and API limits
- **ZIP Compression**: Handles compressed downloads to reduce bandwidth

### Rate Limiting

Qualtrics has API rate limits. Configure retry settings appropriately:
- Small surveys: Use default settings
- Large surveys: Increase `initial_wait_seconds` and `retry_wait_seconds`  
- High volume: Add delays between surveys if needed

## Troubleshooting

### Common Issues

**"No progress ID found"**:
- Check API token permissions
- Verify survey ID exists and is accessible
- Check if survey has any responses

**"File not ready after X attempts"**:
- Increase `max_file_ready_attempts`
- Increase `retry_wait_seconds` for large surveys
- Check Qualtrics API status

**"Invalid response from API"**:
- Verify `url_base` matches your Qualtrics data center
- Check API token is valid and not expired
- Ensure survey is published/active

### Debug Mode

Enable debug logging:
```bash
tap-qualtrics --config config.json --catalog catalog.json --log-level DEBUG
```

Check logs for:
- API request/response details  
- File processing steps
- Partition processing status
- Error stack traces
