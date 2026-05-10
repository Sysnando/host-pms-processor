# Testing with Real ESB + Redis Authentication

This guide explains how to test the pipeline with **real ESB authentication** (Redis + OAuth) while keeping S3 and SQS mocked for local testing.

## Overview

The `LocalTestOrchestrator` now supports two modes:

| Mode | ESB Client | Redis | OAuth | S3 | SQS |
|------|-----------|-------|-------|----|----|
| **Mock** (default) | `MockClimberESBClient` | ❌ | ❌ | Local files | Local logs |
| **Real ESB** | `ClimberESBClient` | ✅ | ✅ | Local files | Local logs |

## Quick Start

### Mock Mode (Default)
No real API calls - everything is mocked:

```bash
python -m tests.test_local_run
```

### Real ESB Mode
Test real ESB authentication with Redis + OAuth:

```bash
USE_REAL_ESB=true python -m tests.test_local_run
```

## Required Environment Variables

### For Mock Mode (Default)
```bash
# Minimal setup - no credentials needed
HOTEL_CODE_S3=QUATRO_VIAS_SA
HOST_API_SUBSCRIPTION_KEY=your-host-pms-key
```

### For Real ESB Mode

**Option 1: Using ESB_BASIC_AUTH (Recommended)**
```bash
# ESB Authentication with pre-encoded Basic Auth
USE_REAL_ESB=true
ESB_BASE_URL=https://qa-esb.climberrms.com:9443
ESB_BASIC_AUTH=base64-encoded-client-id:client-secret
ESB_OAUTH_TOKEN_URL=/oauth2/token
ESB_OAUTH_GRANT_TYPE=client_credentials
```

**Option 2: Using Client ID/Secret (Auto-encoded)**
```bash
# ESB Authentication - will auto-encode to Basic Auth
USE_REAL_ESB=true
ESB_BASE_URL=https://qa-esb.climberrms.com:9443
ESB_OAUTH_CLIENT_ID=your-client-id
ESB_OAUTH_CLIENT_SECRET=your-client-secret
ESB_OAUTH_TOKEN_URL=/oauth2/token
ESB_OAUTH_GRANT_TYPE=client_credentials

# Redis (optional - defaults shown)
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Hotel configuration
HOTEL_CODE_S3=QUATRO_VIAS_SA
HOST_API_SUBSCRIPTION_KEY=your-host-pms-key

# Optional
OUTPUT_DIR=./data_extracts
```

## What Gets Tested

### Mock Mode (USE_REAL_ESB=false)
- ❌ No ESB API calls
- ❌ No Redis connection
- ❌ No OAuth authentication
- ✅ Host PMS API calls (real)
- ✅ Local file saving (S3 mocked)
- ✅ Local message logging (SQS mocked)

### Real ESB Mode (USE_REAL_ESB=true)
- ✅ ESB API endpoints (`/hotels/{code}/parameters`, `/files/register`, etc.)
- ✅ Redis token caching
- ✅ OAuth token fetching and refresh
- ✅ Token expiration handling
- ✅ ESB authentication errors
- ✅ Host PMS API calls (real)
- ✅ Local file saving (S3 mocked)
- ✅ Local message logging (SQS mocked)

## Example Usage

### Test with Specific Hotel
```bash
USE_REAL_ESB=true HOTEL_CODE_S3=QUATRO_VIAS_SA python -m tests.test_local_run
```

### Test with Custom Output Directory
```bash
USE_REAL_ESB=true OUTPUT_DIR=./test_output python -m tests.test_local_run
```

### Test Multiple Hotels (from ESB)
```bash
USE_REAL_ESB=true python -m tests.test_local_run
```

## Starting Redis (for Real ESB Mode)

### Using Docker
```bash
docker run -d -p 6379:6379 redis:alpine
```

### Using Docker Compose
Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
```

Then run:
```bash
docker-compose up -d
```

### Local Redis
```bash
# macOS
brew install redis
brew services start redis

# Linux
sudo apt-get install redis-server
sudo systemctl start redis
```

## Troubleshooting

### Redis Connection Errors
```
Failed to get token from Redis, will fetch new token
```

**Solution**: Redis is gracefully handled - the client will fetch a new token if Redis is unavailable.

To test Redis connectivity:
```bash
redis-cli ping
# Should return: PONG
```

### OAuth Authentication Errors

#### Error: "Unsupported Client Authentication Method!"
```
OAuth token request failed: 401
response_text='{"error_description":"Unsupported Client Authentication Method!","error":"invalid_client"}'
```

**Solution**: The ESB requires Basic Authentication. Use one of these options:

**Option 1: Provide pre-encoded Basic Auth (Recommended)**
```bash
# Encode your credentials first
echo -n "client-id:client-secret" | base64

# Then set the result
ESB_BASIC_AUTH=your-base64-encoded-value
```

**Option 2: Let the code auto-encode**
```bash
ESB_OAUTH_CLIENT_ID=your-client-id
ESB_OAUTH_CLIENT_SECRET=your-client-secret
```

#### Error: "Authentication failed: 401"
```
ESB authentication failed: 401
```

**Solution**: Check your ESB credentials:
- Verify `ESB_BASIC_AUTH` is correct (or `ESB_OAUTH_CLIENT_ID`/`ESB_OAUTH_CLIENT_SECRET`)
- Verify `ESB_BASE_URL` is correct
- Verify `ESB_OAUTH_TOKEN_URL` path is correct (default: `/oauth2/token`)

### 302 Redirect Errors
```
ESB resource not found: 302
```

**Solution**: Make sure you're using `HOTEL_CODE_S3` (not `HOTEL_CODE`) for the hotel code:
```bash
# Correct
HOTEL_CODE_S3=QUATRO_VIAS_SA

# Wrong
HOTEL_CODE=1e2ddc0652be41e1bcc425af2079d50d
```

## Verification

After running with real ESB, verify:

1. **Redis Token Caching**: Check logs for "Using cached OAuth token from Redis"
2. **OAuth Token Fetch**: Check logs for "Successfully fetched OAuth token"
3. **ESB API Calls**: Check logs for "Successfully fetched hotel parameters"
4. **Local Files**: Files saved to `./data_extracts/{hotel_code}_{timestamp}/`

## Benefits of Real ESB Testing

✅ Validate Redis connectivity and configuration
✅ Test OAuth authentication flow end-to-end
✅ Verify token caching and expiration handling
✅ Test actual ESB API endpoints with real responses
✅ Verify fix for 302 redirect issue
✅ No AWS infrastructure required (S3/SQS still mocked)
✅ Easy to toggle between mock and real ESB

## See Also

- [Local Testing README](./README_LOCAL_TESTING.md) - General local testing guide
- [ESB Client](../src/clients/esb_client.py) - Real ESB client implementation
- [Mock ESB Client](../src/clients/mock_esb_client.py) - Mock ESB client
- [Redis Token Manager](../src/clients/redis_token_manager.py) - Token caching logic
