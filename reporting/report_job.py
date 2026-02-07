"""
Independent report script:
Fetch data from a fixed table/query, write CSV or Excel, upload to S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
import argparse
import csv
import os
import sys

from base import BaseConfig, ETlBase, build_parser


class CsvReportWriter:
    def write(self, path: str, rows: Iterable[Dict[str, Any]]) -> int:
        data = list(rows)
        if not data:
            return 0
        fieldnames = self._columns(data)
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow({k: row.get(k) for k in fieldnames})
        return len(data)

    def _columns(self, data: List[Dict[str, Any]]) -> List[str]:
        cols: List[str] = []
        seen = set()
        for row in data:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    cols.append(key)
        return cols


class ExcelReportWriter:
    def write(self, path: str, rows: Iterable[Dict[str, Any]]) -> int:
        try:
            from openpyxl import Workbook  # type: ignore
            from openpyxl.styles import Font  # type: ignore
        except Exception as exc:
            raise RuntimeError("openpyxl is required for Excel reports") from exc

        data = list(rows)
        if not data:
            return 0
        fieldnames = self._columns(data)
        wb = Workbook()
        ws = wb.active
        ws.title = "Report"
        ws.append(fieldnames)
        for row in data:
            ws.append([row.get(k) for k in fieldnames])
        ws.freeze_panes = "A2"
        for cell in ws[1]:
            cell.font = Font(bold=True)
        for i, col in enumerate(fieldnames, start=1):
            max_len = len(str(col))
            for row in data:
                val = row.get(col, "")
                max_len = max(max_len, len(str(val)))
            ws.column_dimensions[chr(64 + i)].width = min(max_len + 2, 40)
        wb.save(path)
        return len(data)

    def _columns(self, data: List[Dict[str, Any]]) -> List[str]:
        cols: List[str] = []
        seen = set()
        for row in data:
            for key in row.keys():
                if key not in seen:
                    seen.add(key)
                    cols.append(key)
        return cols


@dataclass
class ReportJobConfig(BaseConfig):
    report_name: str = "finance_report"
    output_format: str = "csv"  # csv | excel


class ReportJob(ETlBase):
    # Fixed report inputs (not CLI args)
    TABLE = "reporting_finance"
    QUERY = "select * from reporting_finance"
    # For simplicity, we fetch data from resource tables related to this job in the database.
    # Output and destination settings are intentionally fixed inside the program.
    OUTPUT_DIR = "/tmp"
    S3_BUCKET = "my-reports"
    S3_KEY_PREFIX = "finance"
    S3_REGION: Optional[str] = None

    def __init__(self, cfg: ReportJobConfig) -> None:
        super().__init__(cfg)
        self.cfg = cfg

    def _fetch_rows(self) -> Iterable[Dict[str, Any]]:
        try:
            with self.db_cursor() as cur:
                cur.execute(self.QUERY)
                for row in cur.fetchall():
                    yield dict(row)
        except Exception as exc:
            raise RuntimeError("psycopg is required for database access") from exc

    def _output_path(self) -> str:
        ext = "csv" if self.cfg.output_format == "csv" else "xlsx"
        return os.path.join(self.OUTPUT_DIR, f"{self.cfg.report_name}.{ext}")

    def _s3_key(self, output_path: str) -> str:
        filename = os.path.basename(output_path)
        return f"{self.S3_KEY_PREFIX}/{filename}"

    def _writer(self):
        fmt = self.cfg.output_format.lower()
        if fmt == "csv":
            return CsvReportWriter()
        if fmt in {"excel", "xlsx"}:
            return ExcelReportWriter()
        raise ValueError("output_format must be csv or excel")

    def _upload_to_s3(self) -> None:
        try:
            import boto3  # type: ignore
        except Exception as exc:
            raise RuntimeError("boto3 is required for S3 upload") from exc

        output_path = self._output_path()
        s3 = boto3.client("s3", region_name=self.S3_REGION)
        with open(output_path, "rb") as f:
            s3.put_object(Bucket=self.S3_BUCKET, Key=self._s3_key(output_path), Body=f.read())

    def handle(self) -> int:
        self.log.info("report_job_start ts=%s", self.now_utc())
        rows = list(self._fetch_rows())
        writer = self._writer()
        output_path = self._output_path()
        count = writer.write(output_path, rows)
        self._upload_to_s3()
        self.log.info("report_job_end rows=%s ts=%s", count, self.now_utc())
        return count


def add_report_options(p: argparse.ArgumentParser) -> None:
    p.add_argument("--report-name", required=True)
    p.add_argument("--output-format", choices=["csv", "excel"], default="csv")


def _config_from_args(args: argparse.Namespace) -> ReportJobConfig:
    return ReportJobConfig(
        name=args.name,
        log_level=args.log_level,
        dsn=args.dsn,
        retries=args.retries,
        retry_delay_seconds=args.retry_delay_seconds,
        date=args.date,
        report_name=args.report_name,
        output_format=args.output_format,
    )


def main() -> int:
    parser = build_parser("Standalone report job", add_report_options)
    args = parser.parse_args()
    try:
        cfg = _config_from_args(args)
        job = ReportJob(cfg)
        job.selfrun()
        return 0
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
