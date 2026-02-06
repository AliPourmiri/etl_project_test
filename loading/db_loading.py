"""
Database loading stage using BaseStage (PostgreSQL via psycopg).
Inserts rows into a table.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from base import BaseConfig, BaseStage


@dataclass
class DBLoadingConfig(BaseConfig):
    table: str = ""
    columns: Optional[List[str]] = None
    batch_size: int = 1000


class DBLoading(BaseStage):
    def __init__(self, cfg: DBLoadingConfig) -> None:
        # Initialize the base stage and store DB loading config.
        super().__init__(cfg)
        self.cfg = cfg

    def load(self, records: Iterable[Dict[str, Any]]) -> int:
        # Insert records into the configured table and return count.
        if not self.cfg.table:
            raise ValueError("table is required for db loading")
        try:
            import psycopg  # type: ignore
        except Exception as exc:
            raise RuntimeError("psycopg is required for database loading") from exc

        self.logger.info("db_loading_start table=%s ts=%s", self.cfg.table, self.now_utc())
        count = 0
        batch: List[Dict[str, Any]] = []

        def flush(cur) -> None:
            nonlocal count, batch
            if not batch:
                return
            cols = self.cfg.columns or list(batch[0].keys())
            values = [[row.get(c) for c in cols] for row in batch]
            placeholders = ",".join(["%s"] * len(cols))
            col_sql = ",".join(cols)
            sql = f"insert into {self.cfg.table} ({col_sql}) values ({placeholders})"
            cur.executemany(sql, values)
            count += len(batch)
            batch = []

        try:
            with psycopg.connect(self.get_dsn()) as conn:
                with conn.cursor() as cur:
                    for record in records:
                        batch.append(record)
                        if len(batch) >= self.cfg.batch_size:
                            flush(cur)
                    flush(cur)
                conn.commit()
        except Exception as exc:
            self.logger.error("db_loading_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("db_loading_end rows=%s ts=%s", count, self.now_utc())

        return count
