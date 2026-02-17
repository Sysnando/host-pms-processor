"""Application settings and configuration management."""
from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class HotelSettings(BaseSettings):
    """Hotel codes: PMS API vs S3/ESB/SQS paths (Climber padrão)."""

    hotel_code: str = ""  # Code in PMS API (HOTEL_CODE)
    hotel_code_s3: str = ""  # Code in S3/ESB/queue paths (HOTEL_CODE_S3)

    model_config = SettingsConfigDict(env_prefix="HOTEL_")


class AWSSettings(BaseSettings):
    """AWS service configuration."""

    region: str = "eu-west-2"
    s3_raw_prefix: str = "{env}-pms-raw-"
    s3_processed_prefix: str = "{env}-pms-"
    sqs_queue_name: str = "{env}-pms-processor-queue.fifo"
    sqs_queue_url: str = ""  # Optional, will be constructed if empty
    max_retries: int = 3
    request_timeout: int = 30

    # Climber padrão: explicit bucket/queue when set (no AWS_ prefix for these)
    s3_raw_reservations_bucket: str = ""
    s3_reservations_bucket: str = ""
    s3_segments_bucket: str = ""
    sqs_message_group_id: str = ""

    model_config = SettingsConfigDict(env_prefix="AWS_")


class ClimberESBSettings(BaseSettings):
    """Climber ESB API configuration."""

    base_url: str = "https://qa-esb.climberrms.com:9443/oauth2/token"
    api_key: str = "test-api-key-default"  # Default for testing, should be overridden in production
    request_timeout: int = 30
    max_retries: int = 3

    # OAuth configuration
    oauth_token_url: str = "/oauth/token"
    oauth_client_id: str = ""
    oauth_client_secret: str = ""
    oauth_grant_type: str = "client_credentials"

    # Climber padrão: OAuth via Basic Auth + full URLs
    basic_auth: str = ""  # Base64 for token endpoint (ESB_BASIC_AUTH)
    auth_url: str = "https://qa-esb.climberrms.com:9443/oauth2/token"
    reservations_url: str = "https://qa-esb.climberrms.com/pms-integration/1.0/pmsReservation"
    segments_url: str = "https://qa-esb.climberrms.com/pms-integration/1.0/pmsSegment"

    model_config = SettingsConfigDict(env_prefix="ESB_")


class HostPMSSettings(BaseSettings):
    """Host PMS API configuration."""

    base_url: str = "https://hostapi.azure-api.net/rms-v2"
    subscription_key: str = "test-subscription-key-default"  # Default for testing, should be overridden in production
    request_timeout: int = 30
    max_retries: int = 3

    # StatDaily date range configuration
    stat_daily_days_back_start: int = 95  # Days back from today for start date (default: 95 days ago)
    stat_daily_days_back_end: int = 30    # Days back from today for end date (default: 30 days ago)

    model_config = SettingsConfigDict(env_prefix="HOST_API_")


class RedisSettings(BaseSettings):
    """Redis configuration for OAuth token caching."""

    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    ssl: bool = False
    decode_responses: bool = True
    socket_timeout: int = 5
    socket_connect_timeout: int = 5

    model_config = SettingsConfigDict(env_prefix="REDIS_")


class LoggingSettings(BaseSettings):
    """Logging configuration."""

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["json", "console"] = "json"

    model_config = SettingsConfigDict(env_prefix="LOG_")


class Settings(BaseSettings):
    """Main application settings."""

    environment: Literal["dev", "staging", "prod"] = "dev"
    debug: bool = False

    # Single-hotel (Climber padrão): read from .env as HOTEL_CODE / HOTEL_CODE_S3
    hotel_code: str = ""
    hotel_code_s3: str = ""

    # Sub-settings
    hotel: HotelSettings = HotelSettings()
    aws: AWSSettings = AWSSettings()
    esb: ClimberESBSettings = ClimberESBSettings()
    host_pms: HostPMSSettings = HostPMSSettings()
    redis: RedisSettings = RedisSettings()
    logging: LoggingSettings = LoggingSettings()

    # Climber padrão: env vars without prefix (S3_*, SQS_*, etc.)
    s3_raw_reservations_bucket: str = ""
    s3_reservations_bucket: str = ""
    s3_segments_bucket: str = ""
    sqs_queue_url: str = ""
    sqs_message_group_id: str = ""

    # ESB from .env (ESB_AUTH_URL, ESB_BASIC_AUTH, etc.) – used when set to override nested esb.*
    esb_auth_url: str = ""
    esb_reservations_url: str = ""
    esb_segments_url: str = ""
    esb_basic_auth: str = ""

    # Host PMS API from .env (HOST_API_SUBSCRIPTION_KEY, HOST_API_BASE_URL)
    host_api_subscription_key: str = ""
    host_api_base_url: str = ""

    # Feature flags
    store_raw_data: bool = True
    dry_run: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
        extra="ignore",  # Ignore extra env vars (e.g., DATABASE_URL for local testing)
    )

    def validate_climber_padrao(self) -> list[str]:
        """Validate required vars for Climber padrão flow. Returns list of missing var names."""
        missing = []
        code = (self.hotel_code or self.hotel.hotel_code or "").strip()
        code_s3 = (self.hotel_code_s3 or self.hotel.hotel_code_s3 or "").strip()
        if not code:
            missing.append("HOTEL_CODE")
        if not code_s3:
            missing.append("HOTEL_CODE_S3")
        if not self.padrao_raw_bucket():
            missing.append("S3_RAW_RESERVATIONS_BUCKET")
        if not self.padrao_reservations_bucket():
            missing.append("S3_RESERVATIONS_BUCKET")
        if not self.padrao_segments_bucket():
            missing.append("S3_SEGMENTS_BUCKET")
        if not self.padrao_sqs_queue_url():
            missing.append("SQS_QUEUE_URL")
        # SQS_MESSAGE_GROUP_ID optional (defaults to HOTEL_CODE_S3)
        esb_basic = (self.esb_basic_auth or self.esb.basic_auth or "").strip()
        if not esb_basic:
            missing.append("ESB_BASIC_AUTH")
        return missing

    def padrao_raw_bucket(self) -> str:
        """S3 raw reservations bucket (padrão)."""
        return self.s3_raw_reservations_bucket or self.aws.s3_raw_reservations_bucket or ""

    def padrao_reservations_bucket(self) -> str:
        """S3 reservations bucket (padrão)."""
        return self.s3_reservations_bucket or self.aws.s3_reservations_bucket or ""

    def padrao_segments_bucket(self) -> str:
        """S3 segments bucket (padrão)."""
        return self.s3_segments_bucket or self.aws.s3_segments_bucket or ""

    def padrao_sqs_queue_url(self) -> str:
        """SQS queue URL (padrão)."""
        return self.sqs_queue_url or self.aws.sqs_queue_url or ""

    def padrao_sqs_message_group_id(self) -> str:
        """SQS MessageGroupId (padrão). Ignore values that look like .env comments (e.g. '# Optional...')."""
        code_s3 = (self.hotel_code_s3 or self.hotel.hotel_code_s3 or "").strip()
        raw = (self.sqs_message_group_id or self.aws.sqs_message_group_id or "").strip()
        if raw and not raw.startswith("#"):
            return raw
        return code_s3 or ""

    @property
    def aws_s3_raw_prefix(self) -> str:
        """Get S3 raw prefix with environment interpolation."""
        return self.aws.s3_raw_prefix.replace("{env}", self.environment)

    @property
    def aws_s3_processed_prefix(self) -> str:
        """Get S3 processed prefix with environment interpolation."""
        return self.aws.s3_processed_prefix.replace("{env}", self.environment)

    @property
    def aws_sqs_queue_name(self) -> str:
        """Get SQS queue name with environment interpolation."""
        return self.aws.sqs_queue_name.replace("{env}", self.environment)


# Global settings instance
settings = Settings()
