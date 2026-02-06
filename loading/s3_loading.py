"""
S3 loading stage using BaseStage.
Writes records as JSON Lines to an S3 object.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
import io
import json

from base import BaseConfig, BaseStage


@dataclass
class S3LoadingConfig(BaseConfig):
    bucket: str = ""
    key: str = ""
    region: Optional[str] = None


class S3Loading(BaseStage):
    def __init__(self, cfg: S3LoadingConfig) -> None:
        # Initialize the base stage and store S3 config.
        super().__init__(cfg)
        self.cfg = cfg

    def load(self, records: Iterable[Dict[str, Any]]) -> int:
        # Write records as JSON Lines to the configured S3 location.
        if not self.cfg.bucket or not self.cfg.key:
            raise ValueError("bucket and key are required for s3 loading")
        try:
            import boto3  # type: ignore
        except Exception as exc:
            raise RuntimeError("boto3 is required for s3 loading") from exc

        self.logger.info(
            "s3_loading_start bucket=%s key=%s ts=%s",
            self.cfg.bucket,
            self.cfg.key,
            self.now_utc(),
        )

        count = 0
        buf = io.StringIO()
        try:
            for row in records:
                buf.write(json.dumps(row, default=str))
                buf.write("\n")
                count += 1
            s3 = boto3.client("s3", region_name=self.cfg.region)
            s3.put_object(Bucket=self.cfg.bucket, Key=self.cfg.key, Body=buf.getvalue().encode("utf-8"))
        except Exception as exc:
            self.logger.error("s3_loading_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            self.logger.info("s3_loading_end rows=%s ts=%s", count, self.now_utc())
        return count
