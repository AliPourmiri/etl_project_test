"""
Database loading stage using ETlBase (PostgreSQL via psycopg).
Inserts rows into a table.
Includes a CLI that runs ingestion -> validation -> load.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
import argparse
import sys

from base import BaseConfig, ETlBase, build_parser
from ingestion import (
    FileIngestion,
    FileIngestionConfig,
    KafkaIngestion,
    KafkaIngestionConfig,
)
from processing import ValidationProcessing


@dataclass
class DBLoadingConfig(BaseConfig):
    table: str = ""
    columns: Optional[List[str]] = None
    batch_size: int = 1000


class DBLoading(ETlBase):
    def __init__(self, cfg: DBLoadingConfig) -> None:
        # Initialize the base stage and store DB loading config.
        super().__init__(cfg)
        self.cfg = cfg

    def load(self, records: Iterable[Dict[str, Any]]) -> int:
        # Insert records into the configured table and return count.
        if not self.cfg.table:
            raise ValueError("table is required for db loading")

        self.log.info("db_loading_start table=%s ts=%s", self.cfg.table, self.now_utc())
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
            with self.db_cursor() as cur:
                for record in records:
                    batch.append(record)
                    if len(batch) >= self.cfg.batch_size:
                        flush(cur)
                flush(cur)
            self.dbcxn.commit()
        except Exception as exc:
            self.log.error("db_loading_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.log.info("db_loading_end rows=%s ts=%s", count, self.now_utc())

        return count


@dataclass
class DBLoadPipelineConfig(BaseConfig):
    # Ingestion selector: "file", "kafka"
    source: str = "file"
    # File ingestion
    file_path: Optional[str] = None
    file_type: Optional[str] = None
    # Kafka ingestion
    kafka_bootstrap: Optional[str] = None
    kafka_topic: Optional[str] = None
    kafka_group_id: str = "ingestion-consumer"
    kafka_auto_offset_reset: str = "earliest"
    kafka_max_messages: Optional[int] = 100
    # Loading target table
    table: Optional[str] = None
    columns: Optional[list[str]] = None
    batch_size: int = 1000


class DBLoadPipeline(ETlBase):
    def __init__(self, cfg: DBLoadPipelineConfig) -> None:
        # Initialize base stage and store pipeline config.
        super().__init__(cfg)
        self.cfg = cfg

    def _build_ingestion(self):
        # Instantiate the configured ingestion stage.
        if self.cfg.source == "file":
            return FileIngestion(
                FileIngestionConfig(
                    name="file_ingestion",
                    path=self.cfg.file_path or "",
                    file_type=self.cfg.file_type,
                )
            )
        if self.cfg.source == "kafka":
            return KafkaIngestion(
                KafkaIngestionConfig(
                    name="kafka_ingestion",
                    bootstrap_servers=self.cfg.kafka_bootstrap or "",
                    topic=self.cfg.kafka_topic or "",
                    group_id=self.cfg.kafka_group_id,
                    auto_offset_reset=self.cfg.kafka_auto_offset_reset,
                    max_messages=self.cfg.kafka_max_messages,
                )
            )
        raise ValueError(f"Unknown source: {self.cfg.source}")

    def _build_validation(self) -> ValidationProcessing:
        # Instantiate validation stage.
        return ValidationProcessing(self.cfg)

    def _build_loader(self) -> DBLoading:
        # Instantiate DB loader for target table insert.
        return DBLoading(
            DBLoadingConfig(
                name="db_loading",
                table=self.cfg.table or "",
                columns=self.cfg.columns,
                batch_size=self.cfg.batch_size,
            )
        )

    def handle(self) -> int:
        # Run ingestion -> validation -> loading and return loaded count.
        if not self.cfg.table:
            raise ValueError("table is required for db loading")
        self.log.info("db_load_pipeline_start ts=%s", self.now_utc())
        ingestion = self._build_ingestion()
        validator = self._build_validation()
        loader = self._build_loader()
        records = ingestion.read()
        valid_records = list(validator.validate(records))
        count = loader.load(valid_records)
        self.log.info("db_load_pipeline_end loaded=%s ts=%s", count, self.now_utc())
        return count


def _parse_csv_list(value: Optional[str]) -> list[str]:
    # Parse comma-separated CLI list into a list of strings.
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def add_pipeline_options(p: argparse.ArgumentParser) -> None:
    # Ingestion
    p.add_argument("--source", required=True, choices=["file", "kafka"])
    p.add_argument("--file-path")
    p.add_argument("--file-type")
    p.add_argument("--kafka-bootstrap")
    p.add_argument("--kafka-topic")
    p.add_argument("--kafka-group-id", default="ingestion-consumer")
    p.add_argument("--kafka-auto-offset-reset", default="earliest")
    p.add_argument("--kafka-max-messages", type=int, default=100)

    # Loading
    p.add_argument("--table", required=True)
    p.add_argument("--columns", help="Comma-separated list")
    p.add_argument("--batch-size", type=int, default=1000)


def _config_from_args(args: argparse.Namespace) -> DBLoadPipelineConfig:
    # Build DBLoadPipelineConfig from parsed CLI args.
    return DBLoadPipelineConfig(
        name=args.name,
        log_level=args.log_level,
        dsn=args.dsn,
        retries=args.retries,
        retry_delay_seconds=args.retry_delay_seconds,
        date=args.date,
        source=args.source,
        file_path=args.file_path,
        file_type=args.file_type,
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        kafka_group_id=args.kafka_group_id,
        kafka_auto_offset_reset=args.kafka_auto_offset_reset,
        kafka_max_messages=args.kafka_max_messages,
        table=args.table,
        columns=_parse_csv_list(args.columns) or None,
        batch_size=args.batch_size,
    )


def main() -> int:
    # CLI entry point.
    parser = build_parser("DB Loading pipeline CLI", add_pipeline_options)
    args = parser.parse_args()
    try:
        cfg = _config_from_args(args)
        pipeline = DBLoadPipeline(cfg)
        pipeline.selfrun()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
