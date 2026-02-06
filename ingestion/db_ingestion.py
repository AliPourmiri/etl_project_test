"""
Database ingestion stage using BaseStage (PostgreSQL via psycopg).
Executes a query and yields rows as dictionaries.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable

from base import BaseConfig, BaseStage, DBQueryConfig


@dataclass
class DBIngestionConfig(BaseConfig, DBQueryConfig):
    pass


class DBIngestion(BaseStage):
    def __init__(self, cfg: DBIngestionConfig) -> None:
        # Initialize the base stage and store DB config.
        super().__init__(cfg)
        self.cfg = cfg

    def read(self) -> Iterable[Dict[str, Any]]:
        # Execute the SQL query and yield rows as dictionaries.
        if not self.cfg.query:
            raise ValueError("query is required for db ingestion")
        self.logger.info("db_ingestion_start ts=%s", self.now_utc())
        count = 0
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for database ingestion") from exc

        try:
            with psycopg.connect(self.get_dsn()) as conn:
                with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    cur.execute(self.cfg.query)
                    for row in cur.fetchall():
                        count += 1
                        yield dict(row)
        except Exception as exc:
            self.logger.error("db_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("db_ingestion_end rows=%s ts=%s", count, self.now_utc())
