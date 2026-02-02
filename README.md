# Host PMS Connector

A standalone ETL pipeline connector for integrating Host PMS API with AWS services. This connector fetches hotel reservation, configuration, inventory, and financial data from the Host PMS API, transforms it to the Climber standardized JSON format, and stores it in AWS S3 buckets for processing.

## Architecture

```
Host PMS API → Connector → S3 Raw Buckets
                        ↓
                  Transform
                        ↓
                   S3 Processed Buckets → SQS Queue
                        ↓
                   Climber ESB (registration & updates)
                        ↓
                   PMS Processor (database import)
```

### Key Features

- **Standalone Service**: No direct database access - all operations via APIs
- **Incremental Syncing**: Supports `updateFrom` parameter for delta imports
- **Dual S3 Storage**: Raw data for audit trail, processed data for import
- **Financial Filtering**: Only includes room-related transactions (SalesGroup = 0)
- **Segment Management**: Handles unsupported segments with "UNASSIGNED" defaults
- **Error Handling**: Comprehensive retry logic with exponential backoff
- **Structured Logging**: JSON-formatted logs for production environments

## Project Structure

```
host-pms/
├── src/
│   ├── config/              # Configuration and logging
│   │   ├── settings.py      # Pydantic settings
│   │   └── logging.py       # Structlog configuration
│   ├── clients/             # API clients
│   │   ├── host_api_client.py
│   │   └── esb_client.py
│   ├── services/            # Business logic
│   │   ├── extraction_service.py
│   │   ├── transformation_service.py
│   │   └── loading_service.py
│   ├── aws/                 # AWS service wrappers
│   │   ├── s3_manager.py
│   │   └── sqs_manager.py
│   ├── models/              # Data models
│   │   ├── host/            # Host API response models
│   │   └── climber/         # Climber format models
│   ├── transformers/        # Data transformers
│   │   ├── config_transformer.py
│   │   ├── segment_transformer.py
│   │   └── reservation_transformer.py
│   └── main.py              # Application entry point
├── tests/                   # Test suite
├── requirements.txt         # Python dependencies
├── pyproject.toml          # Project configuration
├── Dockerfile              # Container image
├── .env.example            # Environment variables template
└── README.md              # This file
```

## Installation

### Prerequisites

- Python 3.11+
- AWS credentials configured
- Host PMS API subscription key
- Climber ESB API key

### Local Development

1. Clone the repository:
```bash
git clone <repository>
cd host-pms
```

2. Create virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment:
```bash
cp .env.example .env
# Edit .env with your credentials
```

### Docker

Build the container:
```bash
docker build -t host-pms:latest .
```

Run the container:
```bash
docker run \
  -e AWS_REGION=us-east-1 \
  -e ESB_BASE_URL=https://esb.climber.com/api \
  -e ESB_API_KEY=your_key \
  -e HOST_API_SUBSCRIPTION_KEY=your_key \
  host-pms:latest
```

## Configuration

All configuration is managed through environment variables. See `.env.example` for all available options.

### Key Environment Variables

```bash
# Environment
ENVIRONMENT=dev|staging|prod
DEBUG=False

# AWS
AWS_REGION=us-east-1
AWS_S3_RAW_PREFIX={env}-pms-raw-
AWS_S3_PROCESSED_PREFIX={env}-pms-
AWS_SQS_QUEUE_NAME={env}-pms-processor-queue.fifo

# Climber ESB
ESB_BASE_URL=https://esb.climber.com/api
ESB_API_KEY=xxx

# Host PMS API
HOST_API_BASE_URL=https://hostapi.azure-api.net/rms-v2
HOST_API_SUBSCRIPTION_KEY=xxx

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=console|json
```

## Usage

### Running the Connector

```bash
python -m src.main
```

The connector will:
1. Fetch hotel configuration from Climber ESB
2. For each configured hotel:
   - Get import parameters (last import date)
   - Extract data from Host PMS API
   - Transform to Climber standardized format
   - Upload raw data to S3
   - Upload processed data to S3
   - Register files in Climber ESB
   - Update last import date
   - Send SQS trigger message

### First-Time Import

For the first import of a hotel, set the import parameters to:
- `lastImportDate`: `1900-01-01T00:00:00`
- This will fetch all historical data from the PMS

## Data Flow

### 1. Extraction

Fetches data from Host PMS API:
- **Config**: Hotel structure (categories, segments, packages)
- **Reservations**: Room bookings with pricing
- **Inventory**: Room availability and pricing per day
- **Revenue**: Financial transactions (filtered by SalesGroup = 0)

### 2. Transformation

Converts data to Climber standardized JSON format:

```json
{
  "roomInventory": [
    {
      "calendarDate": "[2021-02-02,)",
      "inventory": 1,
      "inventoryOOI": 0,
      "inventoryOOO": 0,
      "roomCode": "D"
    }
  ]
}
```

### 3. Loading

- **Raw Storage**: Original API responses in S3 raw buckets
- **Processed Storage**: Climber format in S3 processed buckets
- **Registration**: Files registered in Climber ESB
- **Notification**: SQS message sent to trigger PMS Processor

## API Integration Details

### Host PMS API Endpoints

- `GET /ExternalRms/Config` - Hotel configuration
- `GET /ExternalRms/Reservation` - Reservations with `updateFrom` parameter
- `GET /Pms/InventoryGrid` - Room availability and pricing
- `GET /ExternalRms/Revenue` - Financial transactions

### Climber ESB API Endpoints

- `GET /hotels` - List available hotels
- `GET /hotels/{code}/parameters` - Import parameters
- `POST /files/register` - Register imported files
- `PUT /hotels/{code}/import-dates` - Update last import date

## Error Handling

The connector implements:
- **Retry Logic**: Up to 3 retries with exponential backoff
- **Partial Success**: Continues processing if individual hotels fail
- **Error Logging**: Comprehensive error logging with context
- **Validation**: Data validation before transformation

## S3 Bucket Structure

### Raw Buckets (Original API responses)
- `{env}-pms-raw-hotel-configs/`
- `{env}-pms-raw-segments/`
- `{env}-pms-raw-reservations/`

### Processed Buckets (Climber standardized format)
- `{env}-pms-hotel-configs/{hotelCode}/config-{timestamp}.json`
- `{env}-pms-segments/{hotelCode}/segments-{timestamp}.json`
- `{env}-pms-reservations/{hotelCode}/reservation-{timestamp}.json`

## Development

### Running Tests

```bash
pytest tests/ -v
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
flake8 src/ tests/

# Sort imports
isort src/ tests/

# Type checking
mypy src/
```

## Deployment

### AWS Lambda

Deploy as a Lambda function triggered by EventBridge (scheduled task):
- Event source: EventBridge rule (e.g., daily at 2 AM)
- Handler: `src.main.lambda_handler`
- Runtime: Python 3.11
- Memory: 1024 MB
- Timeout: 600 seconds

### ECS Fargate

Deploy as a scheduled ECS task:
- Image: `host-pms:latest`
- CPU: 256
- Memory: 512 MB
- Schedule: Daily at 2 AM UTC

## Monitoring

The connector logs all operations using structured logging:

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "logger": "src.services.extraction_service",
  "message": "Successfully extracted 150 reservations",
  "hotel_code": "HOTEL001",
  "count": 150
}
```

Monitor CloudWatch logs for:
- Extraction errors
- Transformation failures
- S3 upload issues
- ESB API failures
- SQS queue errors

## Troubleshooting

### First-time hotel import fails

Check:
1. Hotel code exists in Climber ESB
2. Host PMS API subscription key is valid
3. S3 buckets exist and are accessible
4. IAM permissions allow S3 and SQS operations

### Missing segments

Segments not found in the PMS API response will be mapped to "UNASSIGNED". This is expected behavior for unsupported segment types.

### Revenue data missing

Ensure:
1. Only transactions with `SalesGroup = 0` are included (room revenue)
2. Cancelled reservations have revenue zeroed out
3. Last import date is correctly set for incremental imports

## License

MIT

## Support

For issues or questions, contact the Host Systems development team.
