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
- Pluggable adapters read from files or Kafka (prototype scope).
- Each adapter returns a list of dictionaries.

2. Validation, Quality Checks & Transformation
- A processing stage validates required fields and types.
- This is the extension point for dedupe, referential checks, and enrichment.

3. Storage Layer
- A loading stage inserts validated rows into a staging table.
- The staging table is the source of truth for reporting queries.

4. Reporting Module
- Reports are generated from SQL queries against a predefined table.
- Output format is selected by job settings (CSV or Excel). XML is not generated in this prototype.

**Overview**
This repo provides a modular ETL-style pipeline that:
1. Ingests from file or Kafka.
2. Validates records with schema checks, data quality checks, and basic transformations.
3. Loads validated data into a staging table.
4. Generates a finance report in CSV/XLSX based on the report job settings.

The implementation is intentionally lightweight but designed to be extensible via clear interfaces and separation of concerns.

Example CLI (report job):
```
python reporting/report_job.py \
  --report-name finance_report \
  --output-format excel
```
Note: Some arguments (like table access, permissions, and allowed schemas) can be managed at the database level to simplify CLI usage and allow later changes without code edits.
Note: In this design we use CLI arguments to choose the ingestion type. An alternative is to define resource tables in the database and read ingestion settings from those tables instead of passing arguments.

**Architecture Diagram (Logical)**
```
            +------------------+
            |   Ingestion      |
            | File / Kafka     |
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
            | CSV/XLSX         |
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
  file_ingestion.py       # CSV/JSON/JSONL ingestion
  kafka_ingestion.py      # Kafka consumer ingestion
  s3_ingestion.py         # S3 JSON/JSONL ingestion
processing/
  validation_processing.py # Schema/type validation
loading/
  load_pipeline.py        # DB loader + CLI pipeline
reporting/
  report_job.py           # Standalone report job (DB -> report -> S3)
dags/
  etl_report_dag.py       # Airflow DAG to run load -> report
```

**Key Components**
- `BaseStage` (`base/base.py`): shared logger, UTC timestamps, default DB connection.
- Ingestion:
  - `FileIngestion`: CSV, JSON, JSONL/NDJSON.
  - `KafkaIngestion`: Kafka consumer.
  - `S3Ingestion`: JSON/JSONL reader from S3.
- Processing:
  - `ValidationProcessing`: schema checks, data quality checks, and transformations.
- Loading:
  - `DBLoading`: batch inserts into a table.
- Reporting:
  - `ReportJob`: reads from DB → report generation → S3 upload.

**What’s Implemented vs. Prompt**
Implemented:
- Multi-source ingestion (file, Kafka).
- Validation checks (schema, duplicates, missing values, referential checks).
- Loading to DB.
- Report generation CSV/XLSX.
- CLI for report job.

Not fully implemented (by design, minimal scope):
- XML/XLSX ingestion and XML/JSON report output.
- Advanced data quality checks (dedupe, referential integrity).
- Transformation/enrichment rules.
- Templated report layouts.
- Raw/processed storage separation (can be added with extra loaders).

**How to Run**
Report job (DB → report → S3):
```
python reporting/report_job.py \
  --report-name finance_report \
  --output-format excel
```

**Scheduling (Cron and Airflow)**
Dependency: run the load pipeline first, then run the report job.

Cron example (Linux):
```
# Load at 01:00, then report at 01:15
0 1 * * * /usr/bin/python /path/to/etl_project/loading/load_pipeline.py --source file --file-path /data/input.jsonl --file-type jsonl --table reporting_finance
15 1 * * * /usr/bin/python /path/to/etl_project/reporting/report_job.py --report-name finance_report --output-format excel
```

Airflow example (simple DAG with dependency):
```
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("etl_report", start_date=datetime(2024, 1, 1), schedule="@daily", catchup=False) as dag:
    load_task = BashOperator(
        task_id="load",
        bash_command=(
            "python /path/to/etl_project/loading/load_pipeline.py "
            "--source file --file-path /data/input.jsonl --file-type jsonl "
            "--table reporting_finance"
        ),
    )
    report_task = BashOperator(
        task_id="report",
        bash_command=(
            "python /path/to/etl_project/reporting/report_job.py "
            "--report-name finance_report --output-format excel"
        ),
    )

    load_task >> report_task
```

**Airflow DAG File**
The project includes a ready-to-use Airflow DAG at `dags/etl_report_dag.py`.
You can copy this file into your Airflow `dags/` folder and adjust paths as needed.

**Dependencies**
- `psycopg` for PostgreSQL
- `kafka-python` for Kafka
- `boto3` for S3 upload
- `openpyxl` for Excel reports

**Extending the System**
- Add new ingestion source: implement a new class with a `read()` generator.
- Add new validation rules: extend `ValidationProcessing._is_valid`.
- Add new destination: implement a `load()` class under `loading/`.
- Add new report format: extend `reporting/report_job.py` writers.

**Notes**
The implementation emphasizes clarity and extensibility. All stages use `BaseStage` for consistent logging, timestamps, and DB connectivity.
