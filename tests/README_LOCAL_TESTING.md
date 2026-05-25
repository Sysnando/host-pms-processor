# Local Testing Guide

This directory contains tools for testing the Host PMS Connector pipeline locally without AWS or Climber infrastructure.

## Overview

The local testing setup allows you to:
- ✅ Fetch **real data** from Host PMS API
- ✅ Process data through the **full pipeline**
- ✅ Save files **locally** instead of uploading to S3
- ✅ Log ESB registrations instead of calling API
- ✅ Log SQS messages instead of sending to queue
- ✅ Inspect all generated files for validation

> **Sealed by design:** the local runner always uses mock S3, mock SQS, and mock ESB
> clients. There is no env var or flag to switch to real S3/SQS/ESB — that flow lives
> in `src/main.py` / `HostPMSConnectorOrchestrator`. Running `python -m tests.test_local_run`
> cannot push to production.

## Components

### Mock Clients

Located in `src/aws/` and `src/clients/`:

1. **MockS3Manager** (`src/aws/mock_s3_manager.py`)
   - Saves files to local directory instead of S3
   - Creates directory structure mimicking S3 buckets
   - Logs what would be uploaded
   - Files saved to: `{output_dir}/{bucket_name}/{hotel_code}/{filename}`

2. **MockClimberESBClient** (`src/clients/mock_esb_client.py`)
   - Returns test data for hotel list and parameters
   - Logs file registrations
   - Logs import date updates
   - No actual API calls made

3. **MockSQSManager** (`src/aws/mock_sqs_manager.py`)
   - Logs messages instead of sending to SQS
   - Saves message log to `{output_dir}/sqs_messages.json`
   - Generates mock message IDs

### Test Orchestrator

**LocalTestOrchestrator** (`tests/local_test_orchestrator.py`)
- Extends `HostPMSConnectorOrchestrator`
- Uses mock clients for AWS/ESB operations
- Uses **real** `HostPMSAPIClient` to fetch actual data
- Runs the same 7-step pipeline as production

### Test Script

**test_local_run.py** (`tests/test_local_run.py`)
- Simple script to run local tests
- Supports single hotel or multi-hotel mode
- Configurable output directory
- Prints results and file locations

## Usage

### Single Hotel Mode

Test a specific hotel by setting the `HOTEL_CODE` environment variable:

```bash
# Default output directory (./data_extract)
HOTEL_CODE=HOTEL001 python -m tests.test_local_run

# Custom output directory
OUTPUT_DIR=./my_test python -m tests.test_local_run
```

Or set in `.env` file:
```env
HOTEL_CODE=HOTEL001
```

Then run:
```bash
python -m tests.test_local_run
```

### Multi-Hotel Mode

Test all hotels from the mock list (if no `HOTEL_CODE` is set):

```bash
python -m tests.test_local_run
```

The mock ESB client will return a test list of hotels:
- HOTEL001
- HOTEL002

### Custom Output Directory

Specify where to save the test files:

```bash
OUTPUT_DIR=./test_results HOTEL_CODE=HOTEL001 python -m tests.test_local_run
```

## Output Structure

After running the test, you'll find files organized in hotel-specific timestamped directories:

```
data_extract/
└── HOTEL001_20240115_143022/
    ├── raw_hotel-configs_hotel-configs-20240115_143022.json
    ├── processed_hotel-configs_hotel-configs-20240115_143022.json
    ├── raw_segments_segments-20240115_143022.json
    ├── processed_segments_segments-20240115_143022.json
    ├── raw_reservations_reservations-20240115_143022.json
    ├── processed_reservations_reservations-20240115_143022.json
    └── sqs_messages.json
```

Each test run creates a new directory with the pattern `{hotel_code}_{timestamp}`, containing all files for that specific run.

### File Inspection

You can inspect the generated files to verify:
- ✅ Data is correctly fetched from Host PMS API
- ✅ Transformations are working properly
- ✅ Climber format is correct
- ✅ All required fields are present
- ✅ Date ranges are calculated correctly

## What Gets Mocked vs Real

| Component | Mode | Description |
|-----------|------|-------------|
| **HostPMSAPIClient** | ✅ REAL | Fetches actual data from Host PMS API |
| **S3Manager** | 🔶 MOCKED | Saves files locally, logs uploads |
| **ClimberESBClient** | 🔶 MOCKED | Returns test data, logs registrations |
| **SQSManager** | 🔶 MOCKED | Logs messages, saves to JSON file |
| **Pipeline Steps** | ✅ REAL | Same 7 steps as production |
| **Transformers** | ✅ REAL | Same transformation logic |

## Test Parameters

The mock ESB client returns test parameters:
- **lastImportDate**: 7 days ago
- **minImportDate**: 30 days ago
- **maxImportDate**: today

These can be adjusted in `src/clients/mock_esb_client.py` if needed.

## Example Test Workflow

1. **Set hotel code**:
   ```bash
   export HOTEL_CODE=HOTEL001
   ```

2. **Run test**:
   ```bash
   python -m tests.test_local_run
   ```

3. **Check results**:
   - Review console output for pipeline execution
   - Check `./data_extract/` for generated files
   - Inspect JSON files to verify data correctness

4. **Validate transformations**:
   ```bash
   # Find the latest hotel directory
   HOTEL_DIR=$(ls -td data_extract/HOTEL001_* | head -1)

   # View inventory data
   cat $HOTEL_DIR/processed_hotel-configs_*.json | jq .

   # View reservations
   cat $HOTEL_DIR/processed_reservations_*.json | jq .

   # View SQS messages
   cat $HOTEL_DIR/sqs_messages.json | jq .
   ```

## Troubleshooting

### No files generated

- Check if the pipeline steps are succeeding
- Review logs for error messages
- Verify Host PMS API credentials are correct

### Empty or invalid data

- Check date ranges in mock parameters
- Verify Host PMS API is returning data for those dates
- Review transformer logic if format is incorrect

### Import errors

Make sure to run from the project root directory:
```bash
# From project root
python -m tests.test_local_run

# NOT from tests/ directory
cd tests && python test_local_run.py  # ❌ Won't work
```

## Cleaning Up

Remove test output files:

```bash
rm -rf data_extract/
```

Or clean specific hotel runs:
```bash
rm -rf data_extract/HOTEL001_*/
```

## Production vs Local Testing

To switch between local testing and production:

**Local Testing:**
```bash
python -m tests.test_local_run
```

**Production:**
```bash
python -m src.main
```

The production version uses real S3Manager, ClimberESBClient, and SQSManager.

## Next Steps

After validating locally:
1. ✅ Verify file formats match Climber requirements
2. ✅ Check transformations are correct
3. ✅ Test with different date ranges
4. ✅ Run production pipeline with confidence
