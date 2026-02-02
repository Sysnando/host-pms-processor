"""Application settings and configuration management."""
from typing import Literal, Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class AWSSettings(BaseSettings):
    """AWS service configuration."""

    region: str = "us-east-1"
    s3_raw_prefix: str = "{env}-pms-raw-"
    s3_processed_prefix: str = "{env}-pms-"
    sqs_queue_name: str = "{env}-pms-processor-queue.fifo"
    sqs_queue_url: str = ""  # Optional, will be constructed if empty
    max_retries: int = 3
    request_timeout: int = 30

    model_config = SettingsConfigDict(env_prefix="AWS_")


class ClimberESBSettings(BaseSettings):
    """Climber ESB API configuration."""

    base_url: str = "https://esb.climber.com/api"
    api_key: str = "test-api-key-default"  # Default for testing, should be overridden in production
    request_timeout: int = 30
    max_retries: int = 3

    # OAuth configuration
    oauth_token_url: str = "/oauth/token"
    oauth_client_id: str = ""
    oauth_client_secret: str = ""
    oauth_grant_type: str = "client_credentials"

    model_config = SettingsConfigDict(env_prefix="ESB_")


class HostPMSSettings(BaseSettings):
    """Host PMS API configuration."""

    base_url: str = "https://hostapi.azure-api.net/rms-v2"
    subscription_key: str = "test-subscription-key-default"  # Default for testing, should be overridden in production
    request_timeout: int = 30
    max_retries: int = 3

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

    # Sub-settings
    aws: AWSSettings = AWSSettings()
    esb: ClimberESBSettings = ClimberESBSettings()
    host_pms: HostPMSSettings = HostPMSSettings()
    redis: RedisSettings = RedisSettings()
    logging: LoggingSettings = LoggingSettings()

    # Feature flags
    store_raw_data: bool = True
    dry_run: bool = False

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        case_sensitive=False,
    )

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
