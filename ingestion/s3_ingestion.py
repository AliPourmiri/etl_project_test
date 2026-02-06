"""
S3 ingestion stage using BaseStage.
Reads JSON or JSON Lines objects from S3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable
import json
import os

from base import BaseConfig, BaseStage, S3SourceConfig


@dataclass
class S3IngestionConfig(BaseConfig, S3SourceConfig):
    pass


class S3Ingestion(BaseStage):
    def __init__(self, cfg: S3IngestionConfig) -> None:
        # Initialize the base stage and store S3 config.
        super().__init__(cfg)
        self.cfg = cfg

    def read(self) -> Iterable[Dict[str, Any]]:
        # Read JSON or JSON Lines from S3 and yield records.
        if not self.cfg.bucket or not self.cfg.key:
            raise ValueError("bucket and key are required for s3 ingestion")
        try:
            import boto3  # type: ignore
        except Exception as exc:
            raise RuntimeError("boto3 is required for s3 ingestion") from exc

        self.logger.info(
            "s3_ingestion_start bucket=%s key=%s ts=%s",
            self.cfg.bucket,
            self.cfg.key,
            self.now_utc(),
        )
        count = 0
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
                    count += 1
                    yield json.loads(line)
            elif ext == ".json":
                data = json.loads(body.decode("utf-8"))
                if isinstance(data, list):
                    for item in data:
                        count += 1
                        yield item
                elif isinstance(data, dict):
                    count += 1
                    yield data
                else:
                    raise ValueError("JSON must be an object or array of objects")
            else:
                raise ValueError(f"Unsupported S3 object extension: {ext}")
        except Exception as exc:
            self.logger.error("s3_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("s3_ingestion_end rows=%s ts=%s", count, self.now_utc())
