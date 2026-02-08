"""
File ingestion stage using BaseStage.
Supported: CSV, JSON, JSON Lines (jsonl/ndjson).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable
import csv
import json
import os
import logging

from base import BaseConfig, ETLBase
from ingestion.configs import FileSourceConfig


@dataclass
class FileIngestionConfig(BaseConfig, FileSourceConfig):
    pass


class FileIngestion:
    
    def __init__(self, cfg: FileIngestionConfig, log=logging.getLogger("FileIngestion")) -> None:
        
        self.cfg = cfg
        self.log = log
    
    def _detect_type(self, cfg: FileIngestionConfig) -> str:
        # Infer file type from config or file extension.
        if self.cfg.file_type:
            return self.cfg.file_type.lower()
        _, ext = os.path.splitext(self.cfg.path.lower())
        return ext.lstrip(".")

    def read(self) -> list[Dict[str, Any]]:
        # Read rows from the configured file source into a list.
        if not self.cfg.path:
            raise ValueError("path is required for file ingestion")
        file_type = self._detect_type()
        self.log.info("file_ingestion_start path=%s type=%s ts=%s", self.cfg.path, file_type, self.now_utc())
        rows: list[Dict[str, Any]] = []
        try:
            if file_type == "csv":
                rows = list(self._read_csv())
            elif file_type in {"jsonl", "ndjson"}:
                rows = list(self._read_json_lines())
            elif file_type == "json":
                rows = list(self._read_json())
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        except Exception as exc:
            self.log.error("file_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.log.info("file_ingestion_end rows=%s ts=%s", len(rows), self.now_utc())
        return rows

    def _read_csv(self) -> Iterable[Dict[str, Any]]:
        # Yield rows from a CSV file using the header as field names.
        with open(self.cfg.path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield dict(row)

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
