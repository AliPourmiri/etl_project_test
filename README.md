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
python reporting/report_job.py --name report_job
```
Note: Most configuration is resolved from the resource table keyed by `job_name`. CLI arguments are kept minimal for runtime overrides only.

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
- Configuration Composition: Shared config mixins in `base/base.py` keep common settings consistent, with defaults populated from the resource table.

**Project Structure**
```
base/
  base.py                 # ETlBase (BaseStage) with logging, timestamps, DB connection
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
- `ETlBase` (`base/base.py`): shared logger, UTC timestamps, default DB connection, retries, and CLI helpers.
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
python reporting/report_job.py --name report_job
```

**Scheduling (Cron and Airflow)**
Dependency: run the load pipeline first, then run the report job.

Cron example (Linux):
```
# Load at 01:00, then report at 01:15
0 1 * * * /usr/bin/python /path/to/etl_project/loading/load_pipeline.py --name load_job
15 1 * * * /usr/bin/python /path/to/etl_project/reporting/report_job.py --name report_job
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
        ),
    )
    report_task = BashOperator(
        task_id="report",
        bash_command=(
            "python /path/to/etl_project/reporting/report_job.py "
            
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

**ETlBase Summary**
All classes are extended from `ETlBase` class defined in `base.py`. This class provides methods and attributes that can be used for a configurable ETL process.
It provides the following features:
1. Logging: consistent with the job name with different `log_level` settings.
2. Retries mechanism: the `selfrun()` flow is decorated with a retry mechanism.
3. Resource discovery: configuration details for ingestion/loading/reporting are retrieved from a predefined resource table where job name is the primary key. The table must be set up before triggering a job.
4. Parsing arguments: it provides shared CLI helpers (`add_base_options`, `build_parser`) used for small runtime overrides.
5. Common attributes: date, timestamps, DB connections, etc.

Besides the base class:
- Ingestion classes: each ingestion contains a dataclass responsible for configuration, then a main ingestion class responsible for reading data from sources.

**Resource Table Schema (Example)**
The base class exposes `resource_config()` to fetch a row by `job_name` from a predefined table (default `resource`, overridable via `RESOURCE_TABLE`).
Example schema (PostgreSQL):
```
create table resource (
  job_name text primary key,
  file_path text,
  file_type text,
  s3_bucket_name text,
  s3_key text,
  s3_region text,
  kafka_bootstrap_servers text,
  kafka_topic text,
  kafka_group_id text,
  kafka_auto_offset_reset text,
  kafka_max_messages integer,
  target_table text,
  target_columns text,
  batch_size integer,
  report_name text,
  output_format text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);
```
Add or remove columns as needed for your ingestion/loading/reporting configuration.

**Notes**
The implementation emphasizes clarity and extensibility. All stages use `ETlBase` for consistent logging, timestamps, and DB connectivity.
