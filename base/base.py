"""
Base class for ETL stages (ingestion, processing, loading).
Provides logging, timestamps, and default database connection helpers.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any, Callable, TypeVar
import logging
import os
import time
import argparse

T = TypeVar("T")


def add_base_options(parser: argparse.ArgumentParser) -> None:
    # Base CLI arguments shared by all stages.
    parser.add_argument("--name", default="stage")
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--dsn")
    parser.add_argument("--retries", type=int, default=0)
    parser.add_argument("--retry-delay-seconds", type=float, default=1.0)
    parser.add_argument("--date", help="Optional logical run date (YYYY-MM-DD)")


def build_parser(
    description: Optional[str] = None,
    add_options: Optional[Callable[[argparse.ArgumentParser], None]] = None,
) -> argparse.ArgumentParser:
    # Build an argparse parser and let callers add options.
    parser = argparse.ArgumentParser(description=description or "Stage CLI")
    add_base_options(parser)
    if add_options:
        add_options(parser)
    return parser

@dataclass
class BaseConfig:
    name: str
    log_level: str = "INFO"
    dsn: Optional[str] = None
    retries: int = 0
    retry_delay_seconds: float = 1.0
    date: Optional[str] = None


@dataclass
class BaseStage:
    cfg: BaseConfig
    log: logging.Logger = field(init=False)
    options: dict[str, Any] = field(init=False, default_factory=dict)
    date: Optional[str] = field(init=False, default=None)
    _shared_dbcxn: Optional[Any] = None
    _shared_dsn: Optional[str] = None
    _default_dsn: Optional[str] = "postgresql://user:pass@localhost:5432/etl_db"

    def __post_init__(self) -> None:
        # Initialize stage logger after dataclass construction.
        self.log = self._build_logger(self.cfg.name, self.cfg.log_level)
        self.options = vars(self.cfg)
        self.date = self.cfg.date

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

    def option(self, name: str, default: Optional[Any] = None) -> Any:
        # Fetch an option value from config using a CLI-style name.
        key = name.lstrip("-").replace("-", "_")
        return getattr(self.cfg, key, default)

    def get_dsn(self) -> str:
        # Resolve a DSN from config, environment, or default.
        dsn = self.cfg.dsn or os.environ.get("DATABASE_DSN") or BaseStage._default_dsn
        if not dsn:
            raise ValueError("No DSN provided (cfg.dsn, DATABASE_DSN, or default DSN)")
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
        # Lazily create and reuse a single DB connection across all stages.
        dsn = self.get_dsn()
        if BaseStage._shared_dbcxn is None or BaseStage._shared_dsn != dsn:
            BaseStage._shared_dbcxn = self.connect_postgres()
            BaseStage._shared_dsn = dsn
        return BaseStage._shared_dbcxn

    @property
    def dbCxn(self):
        return self.dbcxn

    def db_cursor(self):
        # Default cursor with dict rows when psycopg is available.
        try:
            import psycopg  # type: ignore
        except Exception:
            return self.dbcxn.cursor()
        return self.dbcxn.cursor(row_factory=psycopg.rows.dict_row)

    def close_dbcxn(self) -> None:
        # Close the shared DB connection if it exists.
        if BaseStage._shared_dbcxn is not None:
            try:
                BaseStage._shared_dbcxn.close()
            finally:
                BaseStage._shared_dbcxn = None
                BaseStage._shared_dsn = None

    @classmethod
    def set_default_dsn(cls, dsn: Optional[str]) -> None:
        # Set a default DSN for all stages (used when cfg.dsn is not set).
        cls._default_dsn = dsn

    def handle(self):
        raise NotImplementedError("Subclasses must implement handle()")

    def selfrun(self):
        # Run the stage with retries when configured.
        return self._with_retries(self.handle, op_name="handle")

    def _with_retries(self, fn: Callable[[], T], op_name: str) -> T:
        attempts = max(1, int(self.cfg.retries) + 1)
        delay = max(0.0, float(self.cfg.retry_delay_seconds))
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except Exception as exc:
                if attempt >= attempts:
                    self.log.error(
                        "%s_failed attempts=%s err=%s ts=%s",
                        op_name,
                        attempt,
                        exc,
                        self.now_utc(),
                    )
                    raise
                self.log.warning(
                    "%s_retry attempt=%s/%s err=%s sleep=%ss ts=%s",
                    op_name,
                    attempt,
                    attempts,
                    exc,
                    delay,
                    self.now_utc(),
                )
                if delay:
                    time.sleep(delay)
