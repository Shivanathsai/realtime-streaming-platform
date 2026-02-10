"""Application configuration with environment variable support."""

import os
from pydantic_settings import BaseSettings
from pydantic import Field


class KafkaConfig(BaseSettings):
    bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    schema_registry_url: str = Field(default="http://localhost:8081", alias="SCHEMA_REGISTRY_URL")
    consumer_group: str = Field(default="streaming-platform", alias="KAFKA_CONSUMER_GROUP")
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 45000

    # Topics
    raw_events_topic: str = "raw-events"
    enriched_events_topic: str = "enriched-events"
    alerts_topic: str = "alerts"
    metrics_topic: str = "metrics"
    deadletter_topic: str = "deadletter"

    # Producer
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 5
    batch_size: int = 16384
    compression_type: str = "snappy"
    idempotent: bool = True  # exactly-once producer

    class Config:
        env_prefix = ""


class PostgresConfig(BaseSettings):
    host: str = Field(default="localhost", alias="POSTGRES_HOST")
    port: int = Field(default=5432, alias="POSTGRES_PORT")
    database: str = Field(default="streaming_platform", alias="POSTGRES_DB")
    user: str = Field(default="postgres", alias="POSTGRES_USER")
    password: str = Field(default="postgres", alias="POSTGRES_PASSWORD")

    @property
    def url(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    class Config:
        env_prefix = ""


class RedisConfig(BaseSettings):
    host: str = Field(default="localhost", alias="REDIS_HOST")
    port: int = Field(default=6379, alias="REDIS_PORT")
    db: int = 0
    ttl_seconds: int = 3600

    class Config:
        env_prefix = ""


class ProcessorConfig(BaseSettings):
    # Windowing
    tumbling_window_seconds: int = 60
    sliding_window_seconds: int = 300
    sliding_window_step_seconds: int = 60
    session_window_gap_seconds: int = 120

    # CEP thresholds
    velocity_threshold: int = 10           # events per window
    amount_spike_threshold: float = 3.0    # std deviations
    geo_velocity_kmh: float = 900.0        # impossible travel speed
    rapid_fire_count: int = 5              # events in 10 seconds
    rapid_fire_window_seconds: int = 10

    # State
    state_ttl_seconds: int = 86400         # 24h state retention

    class Config:
        env_prefix = "PROCESSOR_"


class AppConfig(BaseSettings):
    environment: str = Field(default="development", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    metrics_port: int = Field(default=8000, alias="METRICS_PORT")
    api_port: int = Field(default=8080, alias="API_PORT")

    kafka: KafkaConfig = KafkaConfig()
    postgres: PostgresConfig = PostgresConfig()
    redis: RedisConfig = RedisConfig()
    processor: ProcessorConfig = ProcessorConfig()

    class Config:
        env_prefix = ""


def get_config() -> AppConfig:
    return AppConfig()
