"""
ETL pipeline script:
Ingest -> Validate -> Load to DB table or S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import argparse
import json
import sys

from base import BaseConfig, BaseStage
from ingestion import (
    DBIngestion,
    DBIngestionConfig,
    FileIngestion,
    FileIngestionConfig,
    KafkaIngestion,
    KafkaIngestionConfig,
)
from loading import DBLoading, DBLoadingConfig, S3Loading, S3LoadingConfig
from processing import ValidationProcessing, ValidationProcessingConfig


@dataclass
class PipelineConfig(BaseConfig):
    # Ingestion selector: "file", "kafka", "db"
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
    # DB ingestion
    db_query: Optional[str] = None
    # Validation
    required_fields: Optional[list[str]] = None
    type_map: Optional[dict[str, type]] = None
    allow_extra_fields: bool = True
    max_errors: Optional[int] = None
    # Loading target: "table" or "s3"
    target: str = "table"
    # DB loading
    table: Optional[str] = None
    columns: Optional[list[str]] = None
    batch_size: int = 1000
    # S3 loading
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    s3_region: Optional[str] = None


class ETLPipeline(BaseStage):
    def __init__(self, cfg: PipelineConfig) -> None:
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
        if self.cfg.source == "db":
            return DBIngestion(
                DBIngestionConfig(
                    name="db_ingestion",
                    dsn=self.get_dsn(),
                    query=self.cfg.db_query or "",
                )
            )
        raise ValueError(f"Unknown source: {self.cfg.source}")

    def _build_validation(self) -> ValidationProcessing:
        # Instantiate validation stage.
        return ValidationProcessing(
            ValidationProcessingConfig(
                name="validation",
                required_fields=self.cfg.required_fields or [],
                type_map=self.cfg.type_map or {},
                allow_extra_fields=self.cfg.allow_extra_fields,
                max_errors=self.cfg.max_errors,
            )
        )

    def _build_loader(self):
        # Instantiate the configured loading stage.
        if self.cfg.target == "table":
            return DBLoading(
                DBLoadingConfig(
                    name="db_loading",
                    dsn=self.get_dsn(),
                    table=self.cfg.table or "",
                    columns=self.cfg.columns,
                    batch_size=self.cfg.batch_size,
                )
            )
        if self.cfg.target == "s3":
            return S3Loading(
                S3LoadingConfig(
                    name="s3_loading",
                    bucket=self.cfg.s3_bucket or "",
                    key=self.cfg.s3_key or "",
                    region=self.cfg.s3_region,
                )
            )
        raise ValueError(f"Unknown target: {self.cfg.target}")

    def run(self) -> int:
        # Run ingestion -> validation -> loading and return loaded count.
        self.logger.info("pipeline_start ts=%s", self.now_utc())
        ingestion = self._build_ingestion()
        validator = self._build_validation()
        loader = self._build_loader()
        records = ingestion.read()
        valid_records = validator.validate(records)
        count = loader.load(valid_records)
        self.logger.info("pipeline_end loaded=%s ts=%s", count, self.now_utc())
        return count


def _parse_csv_list(value: Optional[str]) -> list[str]:
    # Parse comma-separated CLI list into a list of strings.
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_type_map(value: Optional[str]) -> dict[str, type]:
    # Parse a JSON mapping of field -> type name into Python types.
    if not value:
        return {}
    raw = json.loads(value)
    if not isinstance(raw, dict):
        raise ValueError("type-map must be a JSON object")
    type_lookup = {"int": int, "float": float, "str": str, "bool": bool}
    out: dict[str, type] = {}
    for k, v in raw.items():
        if not isinstance(v, str) or v not in type_lookup:
            raise ValueError(f"Unsupported type in type-map: {v}")
        out[k] = type_lookup[v]
    return out


def build_parser() -> argparse.ArgumentParser:
    # Build CLI parser for pipeline configuration.
    p = argparse.ArgumentParser(description="ETL pipeline CLI")
    p.add_argument("--name", default="etl_pipeline")
    p.add_argument("--dsn", help="PostgreSQL DSN (or set DATABASE_DSN)")

    # Ingestion
    p.add_argument("--source", required=True, choices=["file", "kafka", "db"])
    p.add_argument("--file-path")
    p.add_argument("--file-type")
    p.add_argument("--kafka-bootstrap")
    p.add_argument("--kafka-topic")
    p.add_argument("--kafka-group-id", default="ingestion-consumer")
    p.add_argument("--kafka-auto-offset-reset", default="earliest")
    p.add_argument("--kafka-max-messages", type=int, default=100)
    p.add_argument("--db-query")

    # Validation
    p.add_argument("--required-fields", help="Comma-separated list")
    p.add_argument("--type-map", help='JSON mapping, e.g. {"id":"int"}')
    p.add_argument("--allow-extra-fields", default="true")
    p.add_argument("--max-errors", type=int)

    # Loading
    p.add_argument("--target", required=True, choices=["table", "s3"])
    p.add_argument("--table")
    p.add_argument("--columns", help="Comma-separated list")
    p.add_argument("--batch-size", type=int, default=1000)
    p.add_argument("--s3-bucket")
    p.add_argument("--s3-key")
    p.add_argument("--s3-region")
    return p


def _parse_bool(value: str) -> bool:
    # Parse boolean CLI values.
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _config_from_args(args: argparse.Namespace) -> PipelineConfig:
    # Build PipelineConfig from parsed CLI args.
    return PipelineConfig(
        name=args.name,
        dsn=args.dsn,
        source=args.source,
        file_path=args.file_path,
        file_type=args.file_type,
        kafka_bootstrap=args.kafka_bootstrap,
        kafka_topic=args.kafka_topic,
        kafka_group_id=args.kafka_group_id,
        kafka_auto_offset_reset=args.kafka_auto_offset_reset,
        kafka_max_messages=args.kafka_max_messages,
        db_query=args.db_query,
        required_fields=_parse_csv_list(args.required_fields),
        type_map=_parse_type_map(args.type_map),
        allow_extra_fields=_parse_bool(args.allow_extra_fields),
        max_errors=args.max_errors,
        target=args.target,
        table=args.table,
        columns=_parse_csv_list(args.columns) or None,
        batch_size=args.batch_size,
        s3_bucket=args.s3_bucket,
        s3_key=args.s3_key,
        s3_region=args.s3_region,
    )


def main() -> int:
    # CLI entry point.
    parser = build_parser()
    args = parser.parse_args()
    try:
        cfg = _config_from_args(args)
        pipeline = ETLPipeline(cfg)
        pipeline.run()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
