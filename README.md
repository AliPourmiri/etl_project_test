**ETL Reporting System — System Design & Basic Implementation**
Context: This project is a response to the interview prompt below. The focus is a clean, extensible design with a minimal but working implementation.

**Interview Prompt (Provided)**
We need a lightweight yet scalable architecture for a reporting system that can ingest data from multiple sources, validate and transform it, persist it for downstream use, and generate reports in various formats. Prepare basic implementation using tools and technologies of your choice, the primary focus is on a clean, extensible system design.

1. Data Ingestion
- File-based ingestion: CSV, JSON, XLSX, XML
- Database queries: relational and analytical stores
- Streaming sources: real-time input from systems like Kafka
This layer should be modular and pluggable so that new input types can be integrated with minimal change.

2. Validation, Quality Checks & Transformation
- Schema validation (field types, required attributes)
- Data quality checks (duplicates, missing values, referential checks)
- Transformations (normalization, enrichment, business-rule mapping)

3. Storage Layer
- Raw data (immutable, for traceability)
- Processed / standardized data (ready for reporting)
Storage choices may include a relational DB, object storage, or a lightweight embedded DB.

4. Reporting Module
- Output formats: CSV, XLSX, PDF, JSON/XML
- Templating support

The final submission should include architecture diagram, documentation, and a basic implementation demonstrating the approach. Minimal implementation is sufficient.

**Overview**
This repo provides a modular ETL-style pipeline that:
1. Ingests from file, Kafka, or database.
2. Validates records with schema and type checks.
3. Loads to destinations (DB or S3).
4. Generates a finance report in CSV/XLSX/PDF based on file extension, then uploads to S3.

The implementation is intentionally lightweight but designed to be extensible via clear interfaces and separation of concerns.

**Architecture Diagram (Logical)**
```
            +------------------+
            |   Ingestion      |
            | File / DB / Kafka|
            +---------+--------+
                      |
                      v
            +------------------+
            |   Processing     |
            | Validation/Rules |
            +---------+--------+
                      |
                      v
            +------------------+
            |   Loading        |
            | DB / S3          |
            +------------------+
                      |
                      v
            +------------------+
            | Reporting        |
            | CSV/XLSX/PDF     |
            +------------------+
                      |
                      v
                 Upload to S3
```

**Design Patterns Used**
- Adapter / Strategy: Each ingestion or loading backend is an interchangeable class with the same interface (`read()` or `load()`).
- Pipeline: Composable stages (ingest → validate → load / report).
- Configuration Composition: Shared config mixins in `base/base.py` keep common settings consistent.

**Project Structure**
```
base/
  base.py                 # BaseStage with logging, timestamps, DB connection
ingestion/
  file_ingestion.py       # CSV/JSON/JSONL/Parquet ingestion
  kafka_ingestion.py      # Kafka consumer ingestion
  db_ingestion.py         # PostgreSQL ingestion
processing/
  validation_processing.py # Schema/type validation
loading/
  db_loading.py           # PostgreSQL loader
  kafka_loading.py        # Kafka producer loader
  s3_loading.py           # S3 object loader (JSONL)
reporting/
  report_writer.py        # Writes CSV/XLSX/PDF based on extension
  report_pipeline.py      # Report pipeline + S3 upload
pipeline.py               # General ETL pipeline CLI
```

**Key Components**
- `BaseStage` (`base/base.py`): shared logger, UTC timestamps, default DB connection.
- Ingestion:
  - `FileIngestion`: CSV, JSON, JSONL/NDJSON, Parquet (streamed).
  - `KafkaIngestion`: Kafka consumer.
  - `DBIngestion`: PostgreSQL query reader.
- Processing:
  - `ValidationProcessing`: required fields + type checks; logs errors.
- Loading:
  - `DBLoading`: batch inserts into a table.
  - `S3Loading`: writes JSONL payload to S3.
  - `KafkaLoading`: produces messages to Kafka (optional).
- Reporting:
  - `ReportWriter`: file extension decides report format.
  - `ReportPipeline`: ingestion → validation → report → S3 upload.

**What’s Implemented vs. Prompt**
Implemented:
- Multi-source ingestion (file, Kafka, DB).
- Validation checks (required fields + type checks).
- Loading to DB or S3.
- Report generation CSV/XLSX/PDF (extension-driven).
- CLI for ETL and report pipelines.

Not fully implemented (by design, minimal scope):
- XML/XLSX ingestion and XML/JSON report output.
- Advanced data quality checks (dedupe, referential integrity).
- Transformation/enrichment rules.
- Templated report layouts.
- Raw/processed storage separation (can be added with extra loaders).

**How to Run**
General ETL pipeline:
```
python pipeline.py \
  --name etl_demo \
  --source file \
  --file-path /data/input.jsonl \
  --file-type jsonl \
  --required-fields id,name \
  --type-map '{"id":"int","name":"str"}' \
  --target table \
  --table public.events
```

Report pipeline (demo finance data → S3):
```
python reporting/report_pipeline.py \
  --name finance_demo \
  --source demo \
  --report-path /tmp/finance_report.xlsx \
  --s3-bucket my-finance-bucket \
  --s3-key reports/finance_report.xlsx \
  --s3-region us-east-1
```

**Dependencies**
- `psycopg` for PostgreSQL
- `kafka-python` for Kafka
- `boto3` for S3
- `openpyxl` for Excel reports
- `reportlab` for PDF reports
- `pyarrow` for Parquet ingestion

**Extending the System**
- Add new ingestion source: implement a new class with a `read()` generator.
- Add new validation rules: extend `ValidationProcessing._is_valid`.
- Add new destination: implement a `load()` class under `loading/`.
- Add new report format: extend `ReportWriter`.

**Notes**
The implementation emphasizes clarity and extensibility. All stages use `BaseStage` for consistent logging, timestamps, and DB connectivity.
