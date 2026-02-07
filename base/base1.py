"""
Minimal base class with options, logger, and db connection.
"""

from dataclasses import dataclass, field
from typing import Optional, Any
from types import SimpleNamespace
import argparse
from datetime import datetime
import logging
import os


@dataclass
class BaseConfig:
    name: str
    log_level: str = "INFO"
    dsn: Optional[str] = None
    date: Optional[str] = None


@dataclass
class BaseStage:
    cfg: BaseConfig
    log: logging.Logger = field(init=False)
    options: SimpleNamespace = field(init=False, default_factory=SimpleNamespace)
    optio: Any = field(init=False)
    option: Any = field(init=False)
    _shared_dbcxn: Optional[Any] = None
    _shared_dsn: Optional[str] = None
    _default_dsn: Optional[str] = "postgresql://user:pass@localhost:5432/etl_db"

    def __post_init__(self) -> None:
        self.log = self._build_logger(self.cfg.name, self.cfg.log_level)
        self.optio = _OptionsBuilder()
        self.option = self.optio

    def _build_logger(self, name: str, log_level: str) -> logging.Logger:
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

    def parse_options(self, argv: Optional[list[str]] = None) -> SimpleNamespace:
        # Parse CLI args into a namespace for dot-access.
        self.options = self.optio.parse(argv)
        if not getattr(self.options, "date", None):
            self.options.date = datetime.now().strftime("%Y-%m-%d")
        if getattr(self.options, "dsn", None):
            self.cfg.dsn = self.options.dsn
        self.cfg.date = self.options.date
        return self.options

    def get_dsn(self) -> str:
        dsn = self.cfg.dsn or os.environ.get("DATABASE_DSN") or self._default_dsn
        if not dsn:
            raise ValueError("No DSN provided (cfg.dsn, DATABASE_DSN, or default DSN)")
        return dsn

    def connect_postgres(self):
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for database connections") from exc
        return psycopg.connect(self.get_dsn())

    @property
    def dbcxn(self):
        dsn = self.get_dsn()
        if BaseStage._shared_dbcxn is None or BaseStage._shared_dsn != dsn:
            BaseStage._shared_dbcxn = self.connect_postgres()
            BaseStage._shared_dsn = dsn
        return BaseStage._shared_dbcxn

    @property
    def dbCxn(self):
        return self.dbcxn

    def close_dbcxn(self) -> None:
        if BaseStage._shared_dbcxn is not None:
            try:
                BaseStage._shared_dbcxn.close()
            finally:
                BaseStage._shared_dbcxn = None
                BaseStage._shared_dsn = None


class _OptionsBuilder:
    def __init__(self) -> None:
        self._parser = argparse.ArgumentParser(add_help=True)
        self._parser.add_argument("--date")
        self._parser.add_argument("--dsn")

    def add_argument(self, *args, **kwargs):
        name = kwargs.pop("name", None)
        if name:
            kwargs["dest"] = name
        return self._parser.add_argument(*args, **kwargs)

    def parse(self, argv: Optional[list[str]] = None) -> SimpleNamespace:
        ns = self._parser.parse_args(argv)
        return SimpleNamespace(**vars(ns))
