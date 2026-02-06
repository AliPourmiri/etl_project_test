"""
File ingestion stage using BaseStage.
Supported: CSV, JSON, JSON Lines (jsonl/ndjson), Parquet (pyarrow).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable
import csv
import json
import os

from base import BaseConfig, BaseStage, FileSourceConfig


@dataclass
class FileIngestionConfig(BaseConfig, FileSourceConfig):
    pass


class FileIngestion(BaseStage):
    def __init__(self, cfg: FileIngestionConfig) -> None:
        # Initialize the base stage and store file config.
        super().__init__(cfg)
        self.cfg = cfg

    def _detect_type(self) -> str:
        # Infer file type from config or file extension.
        if self.cfg.file_type:
            return self.cfg.file_type.lower()
        _, ext = os.path.splitext(self.cfg.path.lower())
        return ext.lstrip(".")

    def read(self) -> Iterable[Dict[str, Any]]:
        # Stream rows from the configured file source.
        if not self.cfg.path:
            raise ValueError("path is required for file ingestion")
        file_type = self._detect_type()
        self.logger.info("file_ingestion_start path=%s type=%s ts=%s", self.cfg.path, file_type, self.now_utc())
        count = 0
        try:
            if file_type == "csv":
                for row in self._read_csv():
                    count += 1
                    yield row
            elif file_type in {"jsonl", "ndjson"}:
                for row in self._read_json_lines():
                    count += 1
                    yield row
            elif file_type == "json":
                for row in self._read_json():
                    count += 1
                    yield row
            elif file_type == "parquet":
                for row in self._read_parquet():
                    count += 1
                    yield row
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as exc:
            self.logger.error("file_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("file_ingestion_end rows=%s ts=%s", count, self.now_utc())

    def _read_csv(self) -> Iterable[Dict[str, Any]]:
        # Yield rows from a CSV file using the header as field names.
        with open(self.cfg.path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield dict(row)

    def _read_json_lines(self) -> Iterable[Dict[str, Any]]:
        # Yield rows from a JSON Lines (one JSON object per line) file.
        with open(self.cfg.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)

    def _read_json(self) -> Iterable[Dict[str, Any]]:
        # Yield rows from a JSON file (list of objects or single object).
        with open(self.cfg.path, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for item in data:
                yield item
        elif isinstance(data, dict):
            yield data
        else:
            raise ValueError("JSON must be an object or array of objects")

    def _read_parquet(self) -> Iterable[Dict[str, Any]]:
        # Yield rows from a Parquet file using pyarrow.
        try:
            import pyarrow.parquet as pq  # type: ignore
        except Exception as exc:
            raise RuntimeError("pyarrow is required for parquet ingestion") from exc

        table = pq.read_table(self.cfg.path)
        for batch in table.to_batches():
            batch_dict = batch.to_pydict()
            keys = list(batch_dict.keys())
            rows = zip(*(batch_dict[k] for k in keys))
            for row in rows:
                yield dict(zip(keys, row))
