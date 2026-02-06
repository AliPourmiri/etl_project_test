"""
Kafka loading stage using BaseStage.
Produces messages to a topic.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional
import json

from base import BaseConfig, BaseStage


@dataclass
class KafkaLoadingConfig(BaseConfig):
    bootstrap_servers: str = ""
    topic: str = ""
    key_field: Optional[str] = None


class KafkaLoading(BaseStage):
    def __init__(self, cfg: KafkaLoadingConfig) -> None:
        # Initialize the base stage and store Kafka loading config.
        super().__init__(cfg)
        self.cfg = cfg

    def load(self, records: Iterable[Dict[str, Any]]) -> int:
        # Produce records to the configured Kafka topic and return count.
        if not self.cfg.bootstrap_servers or not self.cfg.topic:
            raise ValueError("bootstrap_servers and topic are required for kafka loading")
        try:
            from kafka import KafkaProducer  # type: ignore
        except Exception as exc:
            raise RuntimeError("kafka-python is required for kafka loading") from exc

        self.logger.info(
            "kafka_loading_start topic=%s bootstrap=%s ts=%s",
            self.cfg.topic,
            self.cfg.bootstrap_servers,
            self.now_utc(),
        )
        count = 0
        producer = KafkaProducer(
            bootstrap_servers=self.cfg.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        )
        try:
            for record in records:
                key = None
                if self.cfg.key_field and self.cfg.key_field in record:
                    key = str(record[self.cfg.key_field]).encode("utf-8")
                producer.send(self.cfg.topic, value=record, key=key)
                count += 1
            producer.flush()
        except Exception as exc:
            self.logger.error("kafka_loading_error err=%s ts=%s", exc, self.now_utc())
            raise
        finally:
            producer.close()
            self.logger.info("kafka_loading_end rows=%s ts=%s", count, self.now_utc())
        return count
