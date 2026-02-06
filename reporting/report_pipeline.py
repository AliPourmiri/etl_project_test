"""
Report pipeline:
Ingest -> Validate -> Generate Finance Report -> Upload to S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable, Dict, Any
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
    S3Ingestion,
    S3IngestionConfig,
)
from processing import ValidationProcessing, ValidationProcessingConfig
from .report_writer import ReportWriter, ReportWriterConfig


@dataclass
class ReportPipelineConfig(BaseConfig):
    # Ingestion selector: "file", "kafka", "db", "s3", "demo"
    source: str = "demo"
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
    # S3 ingestion
    s3_source_bucket: Optional[str] = None
    s3_source_key: Optional[str] = None
    s3_source_region: Optional[str] = None
    # Validation
    required_fields: Optional[list[str]] = None
    type_map: Optional[dict[str, type]] = None
    allow_extra_fields: bool = True
    max_errors: Optional[int] = None
    # Report output
    report_path: str = "finance_report.csv"
    # S3 destination
    s3_bucket: Optional[str] = None
    s3_key: Optional[str] = None
    s3_region: Optional[str] = None


class ReportPipeline(BaseStage):
    def __init__(self, cfg: ReportPipelineConfig) -> None:
        super().__init__(cfg)
        self.cfg = cfg

    def _build_ingestion(self):
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
        if self.cfg.source == "s3":
            return S3Ingestion(
                S3IngestionConfig(
                    name="s3_ingestion",
                    bucket=self.cfg.s3_source_bucket or "",
                    key=self.cfg.s3_source_key or "",
                    region=self.cfg.s3_source_region,
                )
            )
        if self.cfg.source == "demo":
            return None
        raise ValueError(f"Unknown source: {self.cfg.source}")

    def _build_validation(self) -> ValidationProcessing:
        return ValidationProcessing(
            ValidationProcessingConfig(
                name="validation",
                required_fields=self.cfg.required_fields or [],
                type_map=self.cfg.type_map or {},
                allow_extra_fields=self.cfg.allow_extra_fields,
                max_errors=self.cfg.max_errors,
            )
        )

    def _demo_records(self) -> Iterable[Dict[str, Any]]:
        # Demo finance report dataset.
        return [
            {"account": "Revenue", "period": "2025-Q4", "amount": 1250000.0, "currency": "USD"},
            {"account": "COGS", "period": "2025-Q4", "amount": -430000.0, "currency": "USD"},
            {"account": "Gross Profit", "period": "2025-Q4", "amount": 820000.0, "currency": "USD"},
            {"account": "Opex", "period": "2025-Q4", "amount": -260000.0, "currency": "USD"},
            {"account": "Net Income", "period": "2025-Q4", "amount": 560000.0, "currency": "USD"},
        ]

    def run(self) -> int:
        self.logger.info("report_pipeline_start ts=%s", self.now_utc())
        ingestion = self._build_ingestion()
        validator = self._build_validation()
        if ingestion is None:
            records = self._demo_records()
        else:
            records = ingestion.read()
        valid_records = validator.validate(records)

        writer = ReportWriter(ReportWriterConfig(path=self.cfg.report_path))
        row_count = writer.write(valid_records)

        if self.cfg.s3_bucket and self.cfg.s3_key:
            self._upload_to_s3()
        self.logger.info("report_pipeline_end rows=%s ts=%s", row_count, self.now_utc())
        return row_count

    def _upload_to_s3(self) -> None:
        # Upload the generated report file to S3.
        try:
            import boto3  # type: ignore
        except Exception as exc:
            raise RuntimeError("boto3 is required for S3 upload") from exc

        self.logger.info(
            "report_upload_start bucket=%s key=%s ts=%s",
            self.cfg.s3_bucket,
            self.cfg.s3_key,
            self.now_utc(),
        )
        try:
            s3 = boto3.client("s3", region_name=self.cfg.s3_region)
            with open(self.cfg.report_path, "rb") as f:
                s3.put_object(Bucket=self.cfg.s3_bucket, Key=self.cfg.s3_key, Body=f.read())
        except Exception as exc:
            self.logger.error("report_upload_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("report_upload_end ts=%s", self.now_utc())


def _parse_csv_list(value: Optional[str]) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_type_map(value: Optional[str]) -> dict[str, type]:
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


def _parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Finance report pipeline CLI")
    p.add_argument("--name", default="finance_report_pipeline")
    p.add_argument("--dsn", help="PostgreSQL DSN (or set DATABASE_DSN)")
    p.add_argument("--source", required=True, choices=["file", "kafka", "db", "s3", "demo"])

    p.add_argument("--file-path")
    p.add_argument("--file-type")
    p.add_argument("--kafka-bootstrap")
    p.add_argument("--kafka-topic")
    p.add_argument("--kafka-group-id", default="ingestion-consumer")
    p.add_argument("--kafka-auto-offset-reset", default="earliest")
    p.add_argument("--kafka-max-messages", type=int, default=100)
    p.add_argument("--db-query")
    p.add_argument("--s3-source-bucket")
    p.add_argument("--s3-source-key")
    p.add_argument("--s3-source-region")

    p.add_argument("--required-fields")
    p.add_argument("--type-map")
    p.add_argument("--allow-extra-fields", default="true")
    p.add_argument("--max-errors", type=int)

    p.add_argument("--report-path", required=True)
    p.add_argument("--s3-bucket")
    p.add_argument("--s3-key")
    p.add_argument("--s3-region")
    return p


def _config_from_args(args: argparse.Namespace) -> ReportPipelineConfig:
    return ReportPipelineConfig(
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
        s3_source_bucket=args.s3_source_bucket,
        s3_source_key=args.s3_source_key,
        s3_source_region=args.s3_source_region,
        required_fields=_parse_csv_list(args.required_fields),
        type_map=_parse_type_map(args.type_map),
        allow_extra_fields=_parse_bool(args.allow_extra_fields),
        max_errors=args.max_errors,
        report_path=args.report_path,
        s3_bucket=args.s3_bucket,
        s3_key=args.s3_key,
        s3_region=args.s3_region,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        cfg = _config_from_args(args)
        pipeline = ReportPipeline(cfg)
        pipeline.run()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
