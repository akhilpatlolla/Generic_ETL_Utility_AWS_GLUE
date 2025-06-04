# 🧊 AWS Glue Snowflake ETL Framework

This project implements a scalable and modular ETL (Extract, Transform, Load) framework using **AWS Glue (PySpark)**, **Snowflake**, and **Amazon S3**. It supports multiple source systems, secure parameterization, incremental loads, and automated metrics publishing.

---

## 🚀 Features

- 🔐 Secure credential management via AWS SSM Parameter Store
- ⚙️ Dynamic table-level configuration with support for:
  - Full loads (`FULL`)
  - Incremental loads (`INCREMENTAL`)
  - Truncate + Insert (`TRUNCATE_INSERT`)
  - Merge/Upsert (`UPSERT`)
- 🗃 Automatic schema mapping from Spark to Snowflake
- ☁️ S3-based staging with auto-generated paths
- 📊 CloudWatch metrics for ETL observability
- 🔁 Reusable and extensible ETL utilities for future pipelines

---

## 🧱 Architecture

            ┌────────────┐
            │ AWS SSM    │
            └────┬───────┘
                 ▼
         ┌─────────────────────┐
         │ AWS Glue Job (PySpark)
         └─────────────────────┘
                 │
         ┌───────▼────────┐
         │  Source JDBC   │ <─ MySQL / Postgres / etc.
         └───────┬────────┘
                 ▼
       ┌────────────────────┐
       │ Apply ETL logic    │
       │ (transform/filter) │
       └───────┬────────────┘
               ▼
     ┌────────────────────────┐
     │ Write to Amazon S3     │
     └─────────┬──────────────┘
               ▼   
    ┌─────────────────────────────┐
    │ Merge to Snowflake via SQL  │
    └─────────────────────────────┘
   

---

## 🧾 Configuration Format

ETL configurations are defined per entity/table:

```python
config = {
  'namespace.table_name': {
    'ssmKey': args['SOURCE_KEY'],
    'target_table': 'SNOWFLAKE_SCHEMA.TABLE_NAME',
    'primary_key': 'ID',
    'timestamp_column': 'UPDATED_AT',
    'operations': {
      'type': 'INCREMENTAL',
      'truncate_before_insert': False
    }
  }
}

You can override operation types at runtime via the --OBJ argument:
 
--OBJ '{"namespace.table_name":"INCREMENTAL;_DELTA"}'



💻 Runtime Arguments
| Argument     | Description                             |
| ------------ | --------------------------------------- |
| `JOB_NAME`   | AWS Glue job name                       |
| `SNOWFLAKE`  | SSM path to Snowflake credentials       |
| `AFFILIATE`  | SSM path to source database credentials |
| `PROGRAM360` | Optional connection group               |
| `OPS`        | Optional connection group               |
| `OBJ`        | Operation override per table            |

Output

Files are written to S3 in structured paths:

s3://<bucket>/<job_type>/<database>/<table>/<date>/<timestamp>.csv

Merge logic uses Snowflake's MERGE INTO for upsert operations.


📊 Metrics & Monitoring
This ETL publishes CloudWatch metrics for each table:

Source record count

Filtered (incremental) count

Final Snowflake record count

Row difference

You can use these metrics for dashboards or alerting in CloudWatch.

🧠 Operation Modes
| Mode              | Description                                    |
| ----------------- | ---------------------------------------------- |
| `FULL`            | Full reload of data, truncate before insert    |
| `INCREMENTAL`     | Pull delta using timestamp or PK comparison    |
| `TRUNCATE_INSERT` | Truncate table before re-inserting records     |
| `UPSERT`          | Use Snowflake `MERGE` to insert/update records |
| `APPEND`          | Blind insert of records (no checks)            |

🛡️ Security
All secrets (Snowflake, source DBs) are retrieved from AWS SSM with WithDecryption=True

Jobs assume IAM roles with minimal necessary privileges for SSM, S3, and Glue

📁 Directory Structure

.
├── glue_etl_job.py       # Main ETL driver script
├── README.md             # This documentation
├── config.json (optional)# External config loader (not used directly)

🧩 Extending This Framework
Add new tables: extend the config dictionary

Add custom ETL transformations: extend perform_etl() in the Table class

Add metrics: define new Metric objects

Add support for new data sources: extend the ConnectionConfig class

📋 Requirements
AWS Glue PySpark Runtime (3.0+)

Snowflake Connector for Spark

IAM Role with access to S3, Glue, and SSM

CloudWatch PutMetricData permissions
