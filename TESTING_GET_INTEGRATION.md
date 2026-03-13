# Testing getIntegration Implementation

This guide explains how to test the new getIntegration endpoint implementation with per-hotel credentials.

## Overview

The implementation now:
- Fetches hotels from ESB `getIntegration` endpoint instead of `get_hotels()`
- Uses per-hotel credentials (`auth_password`) as `Ocp-Apim-Subscription-Key`
- Supports both batch processing and single-hotel manual processing

## Prerequisites

1. **Environment Setup**
   ```bash
   # Make sure your .env file has the required settings
   ESB_BASE_URL=https://qa-esb.climberrms.com:9443
   ESB_CLIENT_ID=your_client_id
   ESB_CLIENT_SECRET=your_client_secret
   REDIS_HOST=your_redis_host
   REDIS_PORT=6379
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Test Options

### Option 1: Quick Test - List Hotels

Test fetching the hotel list from getIntegration endpoint:

```bash
python tests/test_get_integration.py --test-fetch
```

This will:
- Call `GET /pms-integration/1.0/getIntegration?integration=BITZ`
- Display all hotels with their credentials
- Show which hotels have valid `auth_password`

**Expected Output:**
```
TEST 1: Fetch Hotels from getIntegration Endpoint
======================================================================

Fetching hotels from getIntegration endpoint...

✓ Successfully fetched 5 hotels

Hotel List:
----------------------------------------------------------------------
Code            Hotel ID     Integration  Has Credentials
----------------------------------------------------------------------
PTFNCTVB        1001511      BITZ         ✓
HOTEL002        1001512      BITZ         ✓
...
```

### Option 2: Test Single Hotel Credentials

Test creating an API client with hotel-specific credentials:

```bash
python tests/test_get_integration.py --test-single PTFNCTVB
```

This will:
- Fetch the hotel from getIntegration
- Extract `auth_password`
- Create `HostPMSAPIClient` with hotel-specific subscription key
- Test calling the Host PMS API

**Expected Output:**
```
TEST 2: Create API Client with Hotel-Specific Credentials (PTFNCTVB)
======================================================================

1. Fetching hotels from getIntegration...
✓ Found hotel: PTFNCTVB
✓ Retrieved subscription key: climber...

2. Creating HostPMSAPIClient with hotel-specific credentials...
✓ Created client:
  - Base URL: https://hostapi.azure-api.net/rms-v2
  - Subscription Key: climber...

3. Testing API call to Host PMS...
✓ Successfully fetched config for: Hotel Test Name
  Hotel Code: PTFNCTVB
```

### Option 3: Dry Run - List All Hotels

List all hotels that would be processed without actually processing them:

```bash
python tests/test_get_integration.py --test-list
```

This will:
- Fetch all hotels from getIntegration
- Display which hotels are ready to process
- Show which hotels are missing credentials

**Expected Output:**
```
TEST 3: List All Hotels (Dry Run)
======================================================================

Fetching hotels from getIntegration endpoint...

✓ Found 5 hotels to process

Hotels that would be processed:
----------------------------------------------------------------------
#     Code            Hotel ID     Status
----------------------------------------------------------------------
1     PTFNCTVB        1001511      ✓ Ready
2     HOTEL002        1001512      ✓ Ready
3     HOTEL003        1001513      ✗ Missing credentials
...
```

### Option 4: Test Invalid Subscription Key

Test how the system handles invalid credentials (should skip the hotel and continue):

```bash
python tests/test_get_integration.py --test-invalid
```

This will:
- Create an API client with an invalid subscription key
- Attempt to process a hotel
- Verify that authentication is validated BEFORE running the pipeline
- Confirm the hotel is skipped (not processed)

**Expected Output:**
```
TEST 4: Invalid Subscription Key Handling
======================================================================

Testing with invalid subscription key...

======================================================================
RESULTS
======================================================================

Success: False
Hotel Code: TEST_HOTEL

Errors (1):
  - Step: authentication
    Type: AUTHENTICATION_FAILED
    Message: Invalid subscription key for hotel TEST_HOTEL. Authentication failed with Host PMS API. Hotel skipped. Please verify credentials in ESB getIntegration endpoint.

✓ Authentication error properly detected and handled!
  Hotel was skipped before running the pipeline.
```

**Key Point**: This test confirms that invalid subscription keys are detected **immediately**, the hotel is **skipped**, and in batch processing, other hotels would continue to be processed.

### Option 5: Full Test - Process Single Hotel

Process a single hotel end-to-end with real API calls (but mock S3/SQS):

```bash
python tests/test_get_integration.py --test-full PTFNCTVB
```

This will:
- Fetch credentials from getIntegration
- Create hotel-specific API client
- Run full pipeline with real Host PMS API calls
- Save files locally to `./data_extracts/PTFNCTVB_*/`

**Expected Output:**
```
TEST 4: Process Single Hotel (PTFNCTVB)
======================================================================

Initializing test orchestrator...
Processing hotel: PTFNCTVB
(Using mock S3 and SQS - files will be saved locally)

[Pipeline execution logs...]

======================================================================
RESULTS
======================================================================

Success: True
Hotel Code: PTFNCTVB

Statistics:
  - config: {'raw_count': 1, 'segments_count': 45}
  - stat_daily: {'raw_record_count': 1234, 'reservations_created': 456}

S3 Uploads (6):
  - config_raw: s3://mock-raw-hotel-configs/PTFNCTVB/...
  - config_processed: s3://mock-processed-hotel-configs/PTFNCTVB/...
  - reservations_raw_1: s3://mock-raw-reservations/PTFNCTVB/...
  - reservations_processed_1: s3://mock-processed-reservations/PTFNCTVB/...

Local Files:
  Directory: ./data_extracts/PTFNCTVB_20260313_143022
    - raw_hotel-configs_hotel-configs-20260313_143023.json (45.2 KB)
    - processed_hotel-configs_hotel-configs-20260313_143023.json (38.1 KB)
    - raw_reservations_reservations-20260313_143024.json (2.3 MB)
    - processed_reservations_reservations-20260313_143024.json (1.8 MB)
```

### Option 6: Run All Tests

Run all tests in sequence:

```bash
python tests/test_get_integration.py
```

This will run:
1. Test fetching integration list
2. Test listing all hotels
3. Test invalid subscription key handling

## Production Usage

### Process Single Hotel Manually

```python
import asyncio
from src.services.orchestration_service import HostPMSConnectorOrchestrator

async def main():
    orchestrator = HostPMSConnectorOrchestrator()

    # Process single hotel - credentials fetched automatically from getIntegration
    result = await orchestrator.process_single_hotel("PTFNCTVB")

    print(f"Success: {result['success']}")

asyncio.run(main())
```

### Process All Hotels

```python
import asyncio
from src.services.orchestration_service import HostPMSConnectorOrchestrator

async def main():
    orchestrator = HostPMSConnectorOrchestrator()

    # Process all hotels from getIntegration endpoint
    results = await orchestrator.process_all_hotels()

    print(f"Total: {results['total_hotels']}")
    print(f"Successful: {results['successful_hotels']}")
    print(f"Failed: {results['failed_hotels']}")

asyncio.run(main())
```

### Using Command Line (main.py)

The `src/main.py` script now automatically uses getIntegration:

```bash
# Process all hotels
python -m src.main

# Process single hotel (set in environment)
export HOTEL_CODE_S3=PTFNCTVB
python -m src.main
```

## Troubleshooting

### Issue: "Hotel not found in integration endpoint"

**Cause:** The hotel code doesn't exist in the getIntegration response.

**Solution:** Check available hotels:
```bash
python tests/test_get_integration.py --test-list
```

### Issue: "No auth_password found for hotel"

**Cause:** The hotel exists but doesn't have credentials configured in ESB.

**Solution:** Contact ESB admin to configure hotel credentials in getIntegration.

### Issue: "Authentication failed: Invalid subscription key"

**Cause:** The `auth_password` from ESB is not valid for Host PMS API.

**Solution:** Verify the credentials in ESB are correct:
```bash
python tests/test_get_integration.py --test-single HOTEL_CODE
```

### Issue: ESB Connection Error

**Cause:** Redis or OAuth credentials are incorrect.

**Solution:** Check your `.env` file:
```bash
# Required for ESB authentication
ESB_BASE_URL=https://qa-esb.climberrms.com:9443
ESB_CLIENT_ID=your_client_id
ESB_CLIENT_SECRET=your_client_secret
REDIS_HOST=your_redis_host
REDIS_PORT=6379
```

## Authentication Failure Handling

The implementation includes **early validation** of subscription keys to fail fast and save resources.

### How It Works

1. **Pre-Pipeline Validation**: Before running the full ETL pipeline, the system validates the subscription key by making a test call to the Host PMS API
2. **Skip on Authentication Failure**: If authentication fails (401 or 403 status), the hotel is skipped and processing continues with the next hotel
3. **No Resource Waste**: The pipeline never starts for hotels with invalid credentials, saving:
   - API calls to Host PMS
   - S3 upload operations
   - ESB registration calls
   - Processing time and resources

### Error Response Format

When authentication fails, the response includes:
```json
{
  "hotel_code": "HOTEL_CODE",
  "success": false,
  "errors": [{
    "step": "authentication",
    "message": "Invalid subscription key for hotel HOTEL_CODE. Authentication failed with Host PMS API. Hotel skipped. Please verify credentials in ESB getIntegration endpoint.",
    "error_type": "AUTHENTICATION_FAILED"
  }],
  "stats": {},
  "s3_uploads": {},
  "sqs_messages": []
}
```

### Batch Processing Behavior

When processing multiple hotels with `process_all_hotels()`:
- **Each hotel is validated independently**
- **Hotels with invalid credentials are SKIPPED, not aborted**
- **Processing continues with the next hotel in the list**
- Authentication failures are tracked separately in `authentication_failures` count
- Final summary includes:
  - `total_hotels`: Total number of hotels
  - `successful_hotels`: Successfully processed
  - `failed_hotels`: All failures (including auth)
  - `authentication_failures`: Specifically auth failures

**Example**: If you have 5 hotels and hotel #2 has invalid credentials:
1. Hotel #1 - Processed successfully ✓
2. Hotel #2 - Skipped (invalid credentials) ✗
3. Hotel #3 - Processed successfully ✓
4. Hotel #4 - Processed successfully ✓
5. Hotel #5 - Processed successfully ✓

**Result**: 4 successful, 1 failed (1 authentication failure)

### Benefits

1. **Fast Skip**: Invalid credentials detected in seconds (one API call), hotel skipped immediately
2. **No Batch Interruption**: Other hotels continue processing normally
3. **Clear Error Messages**: Explicit message about credential issues and that hotel was skipped
4. **Resource Efficiency**: No wasted processing on hotels with invalid credentials
5. **Better Monitoring**: Authentication failures tracked separately for alerting
6. **Graceful Degradation**: System continues working even with some invalid credentials

## Implementation Details

### Files Modified

1. **src/clients/esb_client.py**
   - Added `get_integration(integration_type: str)` method
   - Calls `/pms-integration/1.0/getIntegration?integration=BITZ`

2. **src/clients/host_api_client.py**
   - Modified `__init__(subscription_key: Optional[str] = None)`
   - Accepts per-hotel subscription key

3. **src/services/orchestration_service.py**
   - Updated `process_hotel()` to fetch credentials from getIntegration
   - Added `process_single_hotel()` for manual processing
   - Updated `process_all_hotels()` to use getIntegration endpoint

4. **tests/local_test_orchestrator.py**
   - Updated to support per-hotel credentials

### Backward Compatibility

The implementation is fully backward compatible:
- `HostPMSAPIClient()` without parameters still uses settings
- Existing code continues to work unchanged
- Only new code needs to pass per-hotel credentials

## Next Steps

1. Test with real hotels in QA environment
2. Verify credentials work for each hotel
3. Test in production with monitoring
4. Update documentation for operations team

## Support

For issues or questions, check:
- ESB API documentation
- Host PMS API documentation
- Contact DevOps for ESB credentials
