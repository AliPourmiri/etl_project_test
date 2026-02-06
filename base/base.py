"""
Base class for ETL stages (ingestion, processing, loading).
Provides logging, timestamps, and default database connection helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
import logging
import os


@dataclass
class BaseConfig:
    name: str
    log_level: str = "INFO"
    dsn: Optional[str] = None


@dataclass
class FileSourceConfig:
    path: str = ""
    file_type: Optional[str] = None  # csv, json, jsonl, ndjson, parquet


@dataclass
class KafkaSourceConfig:
    bootstrap_servers: str = ""
    topic: str = ""
    group_id: str = "ingestion-consumer"
    auto_offset_reset: str = "earliest"
    max_messages: Optional[int] = 100


@dataclass
class DBQueryConfig:
    query: str = ""


@dataclass
class S3SourceConfig:
    bucket: str = ""
    key: str = ""
    region: Optional[str] = None


@dataclass
class BaseStage:
    cfg: BaseConfig
    logger: logging.Logger = field(init=False)
    _dbcxn: Optional[object] = field(init=False, default=None)

    def __post_init__(self) -> None:
        # Initialize stage logger after dataclass construction.
        self.logger = self._build_logger(self.cfg.name, self.cfg.log_level)

    @staticmethod
    def _build_logger(name: str, log_level: str) -> logging.Logger:
        # Create or reuse a named logger with a UTC timestamp formatter.
        logger = logging.getLogger(name)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                fmt="%(asctime)s %(levelname)s %(name)s - %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%SZ",
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        logger.setLevel(log_level.upper())
        logger.propagate = False
        return logger

    @staticmethod
    def now_utc() -> datetime:
        # Return the current UTC time for consistent event timestamps.
        return datetime.now(timezone.utc)

    def get_dsn(self) -> str:
        # Resolve a DSN from config or environment for DB connections.
        dsn = self.cfg.dsn or os.environ.get("DATABASE_DSN")
        if not dsn:
            raise ValueError("No DSN provided (cfg.dsn or DATABASE_DSN)")
        return dsn

    def connect_postgres(self):
        # Open a psycopg connection using the resolved DSN.
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for database connections") from exc
        return psycopg.connect(self.get_dsn())

    @property
    def dbcxn(self):
        # Lazily create and reuse a single DB connection per stage.
        if self._dbcxn is None:
            self._dbcxn = self.connect_postgres()
        return self._dbcxn

    def close_dbcxn(self) -> None:
        # Close the cached DB connection if it exists.
        if self._dbcxn is not None:
            try:
                self._dbcxn.close()
            finally:
                self._dbcxn = None
