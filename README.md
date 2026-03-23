# Real-Time Heart Rate ELT Pipeline: Kafka Streaming to Snowflake with Airbyte, dbt, Dagster & Tableau

Streams simulated IoT heart rate data through Kafka into S3, then ingests it into a Snowflake data warehouse using Airbyte with incremental syncs and CDC. Transforms raw data into a star schema with SCD Type 2 dimensions using dbt, orchestrates the full pipeline with Dagster, and visualizes insights in Tableau.

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA GENERATION                                    │
│                                                                                 │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐              │
│   │  Faker Lib   │───>│  Users CSV   │───>│  PostgreSQL on RDS   │              │
│   │  (Python)    │───>│  Activity CSV │───>│  (OLTP Database)     │              │
│   └──────────────┘    └──────────────┘    └──────────┬───────────┘              │
│                                                      │                          │
│   ┌──────────────────────┐                           │                          │
│   │  producer.py         │                           │                          │
│   │  (ECS Container)     │                           │                          │
│   │  Generates heart     │                           │                          │
│   │  rate JSON records   │                           │                          │
│   └──────────┬───────────┘                           │                          │
└──────────────┼───────────────────────────────────────┼──────────────────────────┘
               │                                       │
               ▼                                       │
┌──────────────────────────────┐                       │
│  STREAMING LAYER             │                       │
│                              │                       │
│  Confluent Cloud Kafka       │                       │
│  ┌────────────────────────┐  │                       │
│  │ Topic: heart_rate      │  │                       │
│  │ Partitions: 6          │  │                       │
│  │ Retention: 1 Week      │  │                       │
│  └───────────┬────────────┘  │                       │
│              │               │                       │
│              ▼               │                       │
│  ┌────────────────────────┐  │                       │
│  │ S3 Sink Connector      │  │                       │
│  └───────────┬────────────┘  │                       │
└──────────────┼───────────────┘                       │
               │                                       │
               ▼                                       │
┌──────────────────────────────┐                       │
│  DATA LAKE (S3)              │                       │
│                              │                       │
│  bucket/                     │                       │
│  └── YYYY/MM/DD/HH/         │                       │
│      ├── partition-0.json    │                       │
│      ├── partition-1.json    │                       │
│      └── ...                 │                       │
└──────────────┬───────────────┘                       │
               │                                       │
               ▼                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION (Airbyte on EC2)                            │
│                                                                                 │
│   ┌─────────────────────────┐         ┌─────────────────────────┐               │
│   │ S3 -> Snowflake         │         │ PostgreSQL -> Snowflake │               │
│   │ Mode: Incremental Append│         │ Mode: Incremental Append│               │
│   │ Cursor: file_modified   │         │ With CDC enabled        │               │
│   └────────────┬────────────┘         └────────────┬────────────┘               │
│                │                                   │                            │
│                └──────────────┬─────────────────────┘                            │
└───────────────────────────────┼──────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        DATA WAREHOUSE (Snowflake)                               │
│                                                                                 │
│   raw schema ──> staging models ──> serving models (star schema)                │
│                                                                                 │
│   ┌─────────────────────────────────────────────────────────────┐               │
│   │                    STAR SCHEMA                              │               │
│   │                                                             │               │
│   │   dim_users ──┐                                             │               │
│   │   (SCD Type 2) ├──> fct_heart_rates <──┤ dim_dates          │               │
│   │   dim_activities┘   (clustered by      │ dim_times          │               │
│   │   (SCD Type 2)       event_date)       │                    │               │
│   │                                                             │               │
│   └─────────────────────────────────────────────────────────────┘               │
└──────────────┬──────────────────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────┐
│  VISUALIZATION (Tableau)     │
│                              │
│  Interactive dashboard for   │
│  heart rate analysis by      │
│  demographics and region     │
└──────────────────────────────┘


         ┌──────────────────────────────────────────┐
         │         ORCHESTRATION (Dagster Cloud)     │
         │                                          │
         │  Triggers Airbyte syncs, runs dbt models │
         │  Auto-materialize at 2:30 AM UTC daily   │
         └──────────────────────────────────────────┘
```

---

## What Problem Does This Solve

Heart rate data from wearable devices generates massive volumes of time-series records. Storing this data in a raw operational database makes analytical queries slow and expensive. This pipeline solves that by moving data through a proper ELT workflow: extract from streaming and transactional sources, load into a cloud warehouse, and transform into an optimized star schema that analysts can query efficiently.

**Example questions this pipeline enables:**
- How does heart rate vary across activities for users of different demographics or regions?
- What seasonal patterns exist in heart rate data across user populations?
- Can time-of-day heart rate trends predict activity types within specific regions?

---

## Tech Stack and Responsibilities

```
┌────────────────────┬────────────────────────────────────────────────────────┐
│ Tool               │ What It Does Here                                     │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Python + Faker     │ Generates synthetic user, activity, and heart rate    │
│                    │ data to simulate real IoT wearable output             │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Confluent Kafka    │ Ingests heart rate JSON records in real time across   │
│                    │ 6 partitions with a 1-week retention window           │
├────────────────────┼────────────────────────────────────────────────────────┤
│ AWS S3             │ Persists raw stream data partitioned by event time    │
│                    │ (YYYY/MM/DD/HH) as a replayable data lake            │
├────────────────────┼────────────────────────────────────────────────────────┤
│ AWS RDS (Postgres) │ Hosts the mock OLTP database with user and activity  │
│                    │ reference tables                                      │
├────────────────────┼────────────────────────────────────────────────────────┤
│ AWS ECS + ECR      │ Runs the Kafka producer container continuously       │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Airbyte (on EC2)   │ Incrementally syncs S3 and RDS data into Snowflake   │
│                    │ with CDC for change tracking                          │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Snowflake          │ Cloud data warehouse hosting raw, staging, and       │
│                    │ serving layers                                        │
├────────────────────┼────────────────────────────────────────────────────────┤
│ dbt                │ Transforms raw data into a star schema with SCD      │
│                    │ Type 2 dimensions and comprehensive data quality tests│
├────────────────────┼────────────────────────────────────────────────────────┤
│ Dagster Cloud      │ Orchestrates the full pipeline with asset-based      │
│                    │ scheduling and auto-materialization policies          │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Tableau            │ Provides interactive dashboards for cardiovascular    │
│                    │ health analysis                                       │
├────────────────────┼────────────────────────────────────────────────────────┤
│ Docker             │ Containerizes the Kafka producer for deployment      │
├────────────────────┼────────────────────────────────────────────────────────┤
│ GitHub Actions     │ CI/CD for Dagster Cloud deployments                  │
└────────────────────┴────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
├── .github/workflows/       # Dagster Cloud CI/CD workflows
├── docs/                    # Architecture diagrams, screenshots, performance analysis
├── mock-data/               # Synthetic data generation scripts and CSV outputs
├── orchestrate/             # Dagster assets, resources, and pipeline definitions
├── orchestrate_tests/       # Tests for Dagster orchestration logic
├── transform/               # dbt project (models, seeds, tests, macros)
├── Dockerfile               # Container image for the Kafka producer
├── dagster_cloud.yaml       # Dagster Cloud deployment config
├── pyproject.toml           # Python project configuration
├── setup.py                 # Package setup with Dagster dependencies
└── requirements.txt         # Python dependencies
```

---

## How Each Component Works

### 1. Synthetic Data Generation

Two Python scripts use the Faker library to generate realistic user profiles and activity records. Each script outputs a CSV file that gets uploaded to a PostgreSQL database on AWS RDS. This simulates a production OLTP system that would exist in a real healthcare organization.

```
generate-users ──> users.csv ──> RDS PostgreSQL (users table)
generate-activities ──> activities.csv ──> RDS PostgreSQL (activities table)
```

### 2. Heart Rate Stream Producer

A `producer.py` script runs inside a Docker container on AWS ECS. It continuously generates heart rate records by combining a random user ID, activity ID, heart rate value, GPS coordinates (based on the user country), and a Unix timestamp. Each record is sent as a JSON message to a Kafka topic.

**Record format:**
```json
{
  "user_id": 10001,
  "heart_rate": 128,
  "timestamp": 1711943972,
  "meta": {
    "activity_id": 20890,
    "location": {"latitude": 37.416834, "longitude": -121.975002}
  }
}
```

JSON was chosen over AVRO after benchmarking showed lower storage costs for this specific payload shape.

### 3. Kafka Streaming and S3 Sink

Confluent Cloud hosts the Kafka cluster with a 6-partition topic. A built-in S3 sink connector moves records from Kafka to S3, partitioned by event time in a `YYYY/MM/DD/HH` directory structure. This creates a replayable data lake where raw records can always be reprocessed if something breaks downstream.

```
Kafka Topic (6 partitions)
    │
    ▼
S3 Sink Connector
    │
    ▼
s3://bucket/2024/03/15/14/
    ├── partition-0.json
    ├── partition-1.json
    ├── partition-2.json
    ├── partition-3.json
    ├── partition-4.json
    └── partition-5.json
```

### 4. Ingestion with Airbyte

Airbyte runs on an EC2 instance and handles two sync connections:

**Connection 1: S3 to Snowflake**
- Sync mode: Incremental Append
- Cursor field: `_ab_source_file_last_modified`
- Only picks up new or modified files since the last sync

**Connection 2: PostgreSQL (RDS) to Snowflake**
- Sync mode: Incremental Append (not Append + Deduped)
- CDC enabled to capture inserts, updates, and deletes
- Keeps all historical versions of each record to support SCD Type 2 logic downstream

Both connections land data in the `raw` schema of the Snowflake database.

### 5. Transformation with dbt

The dbt project transforms raw data through three layers:

```
SOURCES (raw schema)          STAGING                    SERVING (star schema)
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│ raw_heart_rates │────>│ stg_heart_rates  │────>│ fct_heart_rates         │
│ raw_users       │────>│ stg_users        │────>│ dim_users (SCD Type 2)  │
│ raw_activities  │────>│ stg_activities   │────>│ dim_activities (SCD2)   │
└─────────────────┘     └──────────────────┘     │ dim_dates               │
                                                 │ dim_times               │
                                                 └─────────────────────────┘
```

**Key design decisions:**

- All staging models use incremental materialization, only processing new rows based on `_airbyte_extracted_at`
- `dim_users` and `dim_activities` implement SCD Type 2 with `start_date` and `end_date` fields, using Airbyte CDC columns to track validity periods
- `fct_heart_rates` joins to dimension tables by checking if the event timestamp falls between the dimension record `start_date` and `end_date`
- The fact table uses `cluster_by=['event_date']` in Snowflake for query performance on date-range filters
- `dim_dates` alone carries 93 data quality tests
- Source-level testing catches data issues before they propagate downstream

### 6. Orchestration with Dagster

Dagster Cloud orchestrates the entire pipeline using asset-based scheduling:

```
Dagster Auto-Materialize (daily at 2:30 AM UTC)
    │
    ├──> Trigger Airbyte sync (S3 to Snowflake)
    ├──> Trigger Airbyte sync (RDS to Snowflake)
    │
    ├──> Run dbt staging models (incremental)
    │
    └──> Run dbt serving models
         ├── dim_users (SCD Type 2 refresh)
         ├── dim_activities (SCD Type 2 refresh)
         └── fct_heart_rates (incremental append)

Note: dim_dates and dim_times are built once and excluded from daily runs
```

The `fct_heart_rates` model has a freshness policy with `maximum_lag_minutes: 1`, and all upstream models use an eager auto-materialize policy, so Dagster automatically runs the full chain when the schedule triggers.

### 7. Tableau Dashboard

An interactive Tableau dashboard visualizes average heart rate during exercise for European users. It allows filtering by demographics, region, and time periods to support cardiovascular health research.

---

## How to Run

### Generate synthetic data
```bash
python -m mock-data.static.scripts.generate-users
python -m mock-data.static.scripts.generate-activities
```

### Run dbt transformations
```bash
# Development
dbt build --target dev

# Production
dbt build --target prod

# Skip expensive dimension tests
dbt build --exclude "dim_dates" "dim_times"
```

### Build and push the producer container
```bash
docker build -t heart-rate-producer .
# Push to ECR and configure ECS task
```

---

## What I Would Improve

- [ ] Connect `producer.py` directly to RDS via SQLAlchemy instead of reading from CSV files
- [ ] Add GitHub Actions for SQL linting, Python linting, and automated dbt tests
- [ ] Add unit tests for the mock data generation scripts
- [ ] Expand `orchestrate_tests` with full coverage
- [ ] Add a dead letter queue for malformed Kafka messages
- [ ] Implement data freshness monitoring and alerting

---

## Known Issues

The EC2-hosted Airbyte instance loses its Docker connector images after a restart. The workaround involves deleting and re-creating the Airbyte directory (source/destination/connection configs persist in memory):

```bash
sudo systemctl start docker
sudo rm -r -f airbyte
mkdir airbyte && cd airbyte
wget https://raw.githubusercontent.com/airbytehq/airbyte/master/run-ab-platform.sh
chmod +x run-ab-platform.sh
./run-ab-platform.sh -b
```

Additionally, since the EC2 instance is not continuously running, the `AIRBYTE_HOST` environment variable in Dagster Cloud must be updated to the new IP address each time the instance relaunches.

---

## Acknowledgments

Inspired by [madyark's heart-rate-stream project](https://github.com/madyark/heart-rate-stream). Built upon and modified for learning purposes.
