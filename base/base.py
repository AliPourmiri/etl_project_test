"""
Minimal ETL base class with CLI options, logging, retry, job_id, and DB connection.
"""

import argparse
import logging
import os
import time
import functools
import uuid
from dataclasses import dataclass
from typing import Optional, Any
from datetime import datetime, date
from pathlib import Path


# -------------------------
# Config (data only)
# -------------------------

@dataclass
class BaseConfig:
    log_level: str = "INFO"
    dsn: Optional[str] = None
    date: Optional[date] = None


# -------------------------
# ETL Base (behavior)
# -------------------------

class ETLBase:
    _default_dsn = "postgresql://user:pass@localhost:5432/etl_db"

    def __init__(self, cfg: BaseConfig, args: Optional[argparse.Namespace] = None):
        self.cfg = cfg
        self.args = args

        self.options: argparse.Namespace
        self.log: logging.Logger
        self._dbcxn: Optional[Any] = None

        # one job_id per ETL object (stable across retries)
        self.job_id: str = uuid.uuid4().hex

        self._init_logging()
        self._init_args()
        self.source = self._init_resource()

    # -------------------------
    # Naming
    # -------------------------

    def name(self) -> str:
        """
        Default job name (script name).
        Subclasses may override.
        """
        try:
            return Path(__file__).stem
        except Exception:
            return self.__class__.__name__

    # -------------------------
    # Retry (class-local)
    # -------------------------

    @staticmethod
    def retry(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            retries = self.options.retries
            delay = self.options.retry_delay

            for attempt in range(retries + 1):
                try:
                    return func(self, *args, **kwargs)
                except Exception:
                    if attempt == retries:
                        self.log.error(
                            "job_id=%s failed after %d retries",
                            self.job_id,
                            retries,
                            exc_info=True,
                        )
                        raise

                    self.log.warning(
                        "job_id=%s retrying (%d/%d)",
                        self.job_id,
                        attempt + 1,
                        retries,
                    )

                    if delay:
                        time.sleep(delay)

        return wrapper

    # -------------------------
    # Initialization helpers
    # -------------------------

    def _init_logging(self) -> None:
        name = self.name()
        self.log = logging.getLogger(name)

        if not self.log.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s - %(message)s"
            ))
            self.log.addHandler(handler)

        self.log.setLevel(self.cfg.log_level.upper())
        self.log.propagate = False

    def _init_args(self) -> None:
        parser = argparse.ArgumentParser(self.name())
        parser.add_argument("--date", type=self._parse_date, help="YYYY-MM-DD")
        parser.add_argument("--dsn", help="Database connection string")

        # retry config
        parser.add_argument("--retries", type=int, default=3)
        parser.add_argument("--retry-delay", type=float, default=0.0)

        # extension hook
        self.add_options(parser)

        self.options = self.args or parser.parse_args()

        # propagate options into config
        if self.options.date:
            self.cfg.date = self.options.date
        if self.options.dsn:
            self.cfg.dsn = self.options.dsn
            
    #-------------------------
    # Resource Discovery 
    #-------------------------
    
    def _init_resource(self):
        query = f"""
        SELECT * FROM etl_resources
        WHERE job_name = '{self.name()}'"""
        with self.dbcxn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            assert rows.count() <= 1, f"Multiple resource configs found for job {self.name()}"
            return rows[0] if rows else None   

    # -------------------------
    # Extension hooks
    # -------------------------

    def add_options(self, parser: argparse.ArgumentParser) -> None:
        """Subclasses may add CLI options."""
        pass

    def handle_job(self) -> None:
        """
        Only required if run() is called.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement handle_job() to use run()"
        )

    @retry
    def run(self) -> None:
        self.log.info(
            "job_id=%s starting execution (date=%s)",
            self.job_id,
            self.cfg.date,
        )
        self.handle_job()

    # -------------------------
    # Database
    # -------------------------

    @property
    def dbcxn(self):
        if self._dbcxn is None:
            try:
                import psycopg  # type: ignore
            except Exception as exc:
                raise RuntimeError("psycopg is required") from exc

            dsn = self.cfg.dsn or os.environ.get("DATABASE_DSN") or self._default_dsn
            if not dsn:
                raise ValueError("No DSN provided")

            self._dbcxn = psycopg.connect(dsn)

        return self._dbcxn

    def close(self) -> None:
        if self._dbcxn:
            self._dbcxn.close()
            self._dbcxn = None

    # -------------------------
    # Helpers
    # -------------------------

    @staticmethod
    def _parse_date(value: str) -> date:
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                "Date must be YYYY-MM-DD"
            ) from exc
