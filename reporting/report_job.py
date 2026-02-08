"""
Independent report script:
Fetch data from a fixed table/query, write CSV or Excel, upload to S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
import argparse
import os
import sys

from base import ETlBase
from .csv_report_writer import CsvReportWriter
from .excel_report_writer import ExcelReportWriter



class ReportJob(ETlBase):
    # Fixed report inputs (not CLI args)
    QUERY = "select * from reporting_finance"
    
    def name(self) -> str:
        return "reporting_job"
    
    def add_options(p: argparse.ArgumentParser) -> None: 
        p.add_argument("--output-format", choices=["csv", "excel"], default="csv")
   


    def _fetch_rows(self) -> Iterable[Dict[str, Any]]:
        try:
            with self.db_cursor() as cur:
                cur.execute(self.QUERY)
                for row in cur.fetchall():
                    yield dict(row)
        except Exception as exc:
            raise RuntimeError("psycopg is required for database access") from exc

    def _output_path(self) -> str:
        ext = "csv" if self.options.output_format == "csv" else "xlsx"
        return os.path.join(self.source.outpt_path, f"{self.source.report_name}.{ext}")

    def _s3_key(self, output_path: str) -> str:
        filename = os.path.basename(output_path)
        return f"{self.source.s3_key}/{filename}"

    def _writer(self):
        fmt = self.options.output_format.lower()
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
            s3.put_object(Bucket=self.source.s3_bucket, Key=self._s3_key(output_path), Body=f.read())

    def handle_job(self) -> int:
        self.log.info("report_job_start ts=%s",self.options.date)
        rows = list(self._fetch_rows())
        writer = self._writer()
        output_path = self._output_path()
        count = writer.write(output_path, rows)
        self._upload_to_s3()
        self.log.info("report_job_end rows=%s ts=%s", count, self.options.date)
        return count


if __name__ == "__main__":
        job = ReportJob()
        job.run()


