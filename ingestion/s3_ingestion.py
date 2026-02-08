"""
S3 ingestion stage using BaseStage.
Reads JSON or JSON Lines objects from S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable
import json
import os

from base import BaseConfig, BaseStage
from ingestion.configs import S3SourceConfig
import logging


@dataclass
class S3IngestionConfig(BaseConfig, S3SourceConfig):
    pass


class S3Ingestion:
    
    def __init__(self, cfg: S3IngestionConfig, log=logging.getLogger("S3ingestion")) -> None:
        # Initialize the base stage and store S3 config.
        self.cfg = cfg

    def read(self) -> list[Dict[str, Any]]:
        # Read JSON or JSON Lines from S3 into a list of records.
        if not self.cfg.bucket or not self.cfg.key:
            raise ValueError("bucket and key are required for s3 ingestion")
        try:
            import boto3  # type: ignore
        except Exception as exc:
            raise RuntimeError("boto3 is required for s3 ingestion") from exc

        self.log.info(
            "s3_ingestion_start bucket=%s key=%s ts=%s",
            self.cfg.bucket,
            self.cfg.key,
            self.now_utc(),
        )
        rows: list[Dict[str, Any]] = []
        try:
            s3 = boto3.client("s3", region_name=self.cfg.region)
            obj = s3.get_object(Bucket=self.cfg.bucket, Key=self.cfg.key)
            body = obj["Body"].read()
            ext = os.path.splitext(self.cfg.key.lower())[1]
            if ext in {".jsonl", ".ndjson"}:
                for line in body.decode("utf-8").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    rows.append(json.loads(line))
            elif ext == ".json":
                data = json.loads(body.decode("utf-8"))
                if isinstance(data, list):
                    rows.extend(data)
                elif isinstance(data, dict):
                    rows.append(data)
                else:
                    raise ValueError("JSON must be an object or array of objects")
            else:
                raise ValueError(f"Unsupported S3 object extension: {ext}")
        except Exception as exc:
            self.log.error("s3_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.log.info("s3_ingestion_end rows=%s ts=%s", len(rows), self.now_utc())
        return rows
