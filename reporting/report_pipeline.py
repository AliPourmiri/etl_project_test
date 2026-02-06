"""
Report pipeline:
Ingest -> Validate -> Generate Finance Report -> Upload to S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import argparse
import json
import sys

from base import BaseConfig, BaseStage
from ingestion import DBIngestion, DBIngestionConfig
from loading import DBLoading, DBLoadingConfig
from processing import ValidationProcessing, ValidationProcessingConfig
from .report_writer import ReportWriter, ReportWriterConfig


@dataclass
class ReportPipelineConfig(BaseConfig):
    # DB ingestion
    db_query: Optional[str] = None
    # Load to table before reporting
    load_table: Optional[str] = None
    load_columns: Optional[list[str]] = None
    load_batch_size: int = 1000
    # Report query (defaults to select * from load_table)
    report_query: Optional[str] = None
    # Validation
    required_fields: Optional[list[str]] = None
    type_map: Optional[dict[str, type]] = None
    allow_extra_fields: bool = True
    max_errors: Optional[int] = None
    # Report output
    report_path: str = "finance_report.csv"


class ReportPipeline(BaseStage):
    def __init__(self, cfg: ReportPipelineConfig) -> None:
        super().__init__(cfg)
        self.cfg = cfg

    def _build_ingestion(self):
        return DBIngestion(
            DBIngestionConfig(
                name="db_ingestion",
                dsn=self.get_dsn(),
                query=self.cfg.db_query or "",
            )
        )

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

    def _build_loader(self) -> DBLoading:
        # Instantiate DB loader for staging table insert.
        return DBLoading(
            DBLoadingConfig(
                name="db_loading",
                dsn=self.get_dsn(),
                table=self.cfg.load_table or "",
                columns=self.cfg.load_columns,
                batch_size=self.cfg.load_batch_size,
            )
        )

    def _build_report_ingestion(self) -> DBIngestion:
        # Read back from the loaded table to generate the report.
        query = self.cfg.report_query or f"select * from {self.cfg.load_table}"
        return DBIngestion(
            DBIngestionConfig(
                name="db_report_ingestion",
                dsn=self.get_dsn(),
                query=query,
            )
        )

    def run(self) -> int:
        self.logger.info("report_pipeline_start ts=%s", self.now_utc())
        ingestion = self._build_ingestion()
        validator = self._build_validation()
        records = ingestion.read()
        valid_records = validator.validate(records)

        if not self.cfg.load_table:
            raise ValueError("load_table is required to load data before reporting")
        loader = self._build_loader()
        loader.load(valid_records)

        report_ingestion = self._build_report_ingestion()
        writer = ReportWriter(ReportWriterConfig(path=self.cfg.report_path))
        row_count = writer.write(report_ingestion.read())
        self.logger.info("report_pipeline_end rows=%s ts=%s", row_count, self.now_utc())
        return row_count


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
    p.add_argument("--db-query")
    p.add_argument("--load-table")
    p.add_argument("--load-columns")
    p.add_argument("--load-batch-size", type=int, default=1000)
    p.add_argument("--report-query")

    p.add_argument("--required-fields")
    p.add_argument("--type-map")
    p.add_argument("--allow-extra-fields", default="true")
    p.add_argument("--max-errors", type=int)

    p.add_argument("--report-path", required=True)
    return p


def _config_from_args(args: argparse.Namespace) -> ReportPipelineConfig:
    return ReportPipelineConfig(
        name=args.name,
        dsn=args.dsn,
        db_query=args.db_query,
        load_table=args.load_table,
        load_columns=_parse_csv_list(args.load_columns) or None,
        load_batch_size=args.load_batch_size,
        report_query=args.report_query,
        required_fields=_parse_csv_list(args.required_fields),
        type_map=_parse_type_map(args.type_map),
        allow_extra_fields=_parse_bool(args.allow_extra_fields),
        max_errors=args.max_errors,
        report_path=args.report_path,
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if not args.db_query:
            raise ValueError("--db-query is required for report pipeline")
        if not args.load_table:
            raise ValueError("--load-table is required for report pipeline")
        cfg = _config_from_args(args)
        pipeline = ReportPipeline(cfg)
        pipeline.run()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
