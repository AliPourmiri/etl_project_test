from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class FileSourceConfig:
    path: str = ""
    file_type: Optional[str] = None  # csv, json, jsonl, ndjson, parquet


@dataclass
class KafkaSourceConfig:
    bootstrap_servers: str = ""
    topic: str = ""
    group_id: str = "ingestion-consumer"
    auto_offset_reset: str = "earliest"
    max_messages: Optional[int] = 100


@dataclass
class S3SourceConfig:
    bucket: str = ""
    key: str = ""
    region: Optional[str] = None
