"""
Kafka ingestion stage using BaseStage.
Consumes messages and yields deserialized payloads.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable
import json
import logging

from base import BaseConfig, BaseStage
from ingestion.configs import KafkaSourceConfig


@dataclass
class KafkaIngestionConfig(BaseConfig, KafkaSourceConfig):
    pass


class KafkaIngestion:
    
    def __init__(self, cfg: KafkaIngestionConfig, log=logging.getLogger("KafkaIngestion")) -> None:
        # Initialize the base stage and store Kafka config.
        self.cfg = cfg
        self.log = log

    def read(self) -> list[Dict[str, Any]]:
        # Consume messages from Kafka and return a list of payloads.
        if not self.cfg.bootstrap_servers or not self.cfg.topic:
            raise ValueError("bootstrap_servers and topic are required for kafka ingestion")
        try:
            from kafka import KafkaConsumer  # type: ignore
        except Exception as exc:
            raise RuntimeError("kafka-python is required for kafka ingestion") from exc

        self.log.info(
            "kafka_ingestion_start topic=%s bootstrap=%s ts=%s",
            self.cfg.topic,
            self.cfg.bootstrap_servers,
            self.now_utc(),
        )

        consumer = KafkaConsumer(
            self.cfg.topic,
            bootstrap_servers=self.cfg.bootstrap_servers,
            group_id=self.cfg.group_id,
            auto_offset_reset=self.cfg.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=self._deserialize,
        )

        rows: list[Dict[str, Any]] = []
        try:
            for msg in consumer:
                rows.append(msg)
                if self.cfg.max_messages is not None and len(rows) >= self.cfg.max_messages:
                    break
        except Exception as exc:
            self.log.error("kafka_ingestion_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            consumer.close()
            self.log.info("kafka_ingestion_end rows=%s ts=%s", len(rows), self.now_utc())
        return rows

    def _deserialize(self, payload: bytes) -> Dict[str, Any]:
        # Decode JSON if possible; otherwise return raw payload text.
        try:
            return json.loads(payload.decode("utf-8"))
        except Exception:
            return {"raw": payload.decode("utf-8", errors="replace")}
