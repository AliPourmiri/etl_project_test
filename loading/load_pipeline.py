"""
Database loading stage using BaseStage (PostgreSQL via psycopg).
Inserts rows into a table.
Includes a CLI that runs ingestion -> validation -> load.
"""

from __future__ import annotations


import argparse
from base import  ETLBase
from ingestion.file_ingestion import FileIngestion, FileIngestionConfig
from ingestion.kafka_ingestion import KafkaIngestion, KafkaIngestionConfig
from ingestion.s3_ingestion import S3Ingestion, S3IngestionConfig  
from processing.validation_processing import ValidationProcessing
from ingestion import (
    FileIngestion,
    FileIngestionConfig,
    KafkaIngestion,
    KafkaIngestionConfig,
)
from processing import ValidationProcessing


class DBLoadPipeline(ETLBase):
        
    def name(self) -> str:
        return "db_load_pipeline.py"
    
    def add_options(p: argparse.ArgumentParser) -> None:
    # Ingestion
        p.add_argument("--source", required=True, choices=["file", "kafka" , "s3"])
    # Loading
        p.add_argument("--target", default="postgresql", choices=["postgresql"])
        p.add_argument("--batch-size", type=int, default=1000)
    
    def _build_ingestion(self):
        # Instantiate the configured ingestion stage.
        if self.options.source == "file":
            return FileIngestion(
                FileIngestionConfig(
                    name="file_ingestion",
                    path=self.resouce.file_path or "",
                    file_type=self.resource.file_type,
                    log = self.log
                )
            )
        elif self.options.source == "kafka":
            return KafkaIngestion(
                KafkaIngestionConfig(
                    name="kafka_ingestion",
                    bootstrap_servers=self.resource.kafka_bootstrap or "",
                    topic=self.resource.kafka_topic or "",
                    group_id=self.resource.kafka_group_id,
                    auto_offset_reset=self.resource.kafka_auto_offset_reset,
                    max_messages=self.resource.kafka_max_messages,
                log = self.log)
            )
        elif self.options.source == "s3":
            
            return S3Ingestion(
                S3IngestionConfig(
                    name="s3_ingestion",
                    bucket=self.resource.s3_bucket or "",
                    key=self.resource.s3_key or "",
                    file_type=self.resource.file_type,
                    log = self.log
                )
            )
            
        raise ValueError(f"Unknown source: {self.options.source}")

    def _build_validation(self) -> ValidationProcessing:
        # Instantiate validation stage.
        return ValidationProcessing()


    def handle_job(self) -> int:
        # Run ingestion -> validation -> loading and return loaded count.
        if not self.options.table:
            raise ValueError("table is required for db loading")
        self.log.info("db_load_pipeline_start ts=%s", self.now_utc())
        ingestion = self._build_ingestion()
        validator = self._build_validation()
        records = ingestion.read()
        valid_records = list(validator.validate(records))
        # Load into database, it can be more efficient.
        count = 0
        for rec in valid_records:
            self.ddCxn.curor().execute(f"Insert to table values {rec}")  # simple query to check DB connection
            count += 1
        self.log.info("db_load_pipeline_end loaded=%s ts=%s", count, self.now_utc())
        

if __name__ == "__main__":
    class_to_run = DBLoadPipeline()
    class_to_run.run()
