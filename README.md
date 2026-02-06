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

**High-Level Design View (1–4)**
1. Data Ingestion
- Pluggable adapters read from files, Kafka, or a database query.
- Each adapter yields records as generators for streaming.

2. Validation, Quality Checks & Transformation
- A processing stage validates required fields and types.
- This is the extension point for dedupe, referential checks, and enrichment.

3. Storage Layer
- A loading stage inserts validated rows into a staging table.
- The staging table is the source of truth for reporting queries.

4. Reporting Module
- Reports are generated from SQL queries against the staging table.
- Output format is selected by report file extension (CSV/XLSX/PDF).

**Overview**
This repo provides a modular ETL-style pipeline that:
1. Ingests from file, Kafka, or database.
2. Validates records with schema and type checks.
3. Loads validated data into a staging table.
4. Generates a finance report in CSV/XLSX/PDF based on file extension.

The implementation is intentionally lightweight but designed to be extensible via clear interfaces and separation of concerns.

Example CLI (report pipeline):
```
python reporting/report_pipeline.py \
  --name finance_report \
  --dsn "postgresql://user:pass@host/db" \
  --db-query "select account, period, amount, currency from finance_source" \
  --load-table reporting_finance \
  --report-path /tmp/finance_report.xlsx
```
Note: Some arguments (like table access, permissions, and allowed schemas) can be managed at the database level to simplify CLI usage and allow later changes without code edits.

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
            | DB (Staging)     |
            +------------------+
                      |
                      v
            +------------------+
            | Reporting        |
            | CSV/XLSX/PDF     |
            +------------------+
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
reporting/
  report_writer.py        # Writes CSV/XLSX/PDF based on extension
  report_pipeline.py      # Report pipeline (DB -> validate -> load -> report)
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
- Reporting:
  - `ReportWriter`: file extension decides report format.
  - `ReportPipeline`: reads from DB → validation → load → report generation.

**What’s Implemented vs. Prompt**
Implemented:
- Multi-source ingestion (file, Kafka, DB).
- Validation checks (required fields + type checks).
- Loading to DB.
- Report generation CSV/XLSX/PDF (extension-driven).
- CLI for report pipeline.

Not fully implemented (by design, minimal scope):
- XML/XLSX ingestion and XML/JSON report output.
- Advanced data quality checks (dedupe, referential integrity).
- Transformation/enrichment rules.
- Templated report layouts.
- Raw/processed storage separation (can be added with extra loaders).

**How to Run**
Report pipeline (DB → validate → load → report):
```
python reporting/report_pipeline.py \
  --name finance_report \
  --dsn "postgresql://user:pass@host/db" \
  --db-query "select account, period, amount, currency from finance_source" \
  --load-table reporting_finance \
  --report-path /tmp/finance_report.xlsx
```

**Dependencies**
- `psycopg` for PostgreSQL
- `kafka-python` for Kafka
- `boto3` for optional S3 ingestion
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
