# Chicago Taxi Data Pipeline

## Current milestone: Airflow + Spark + MinIO

This milestone runs Airflow, its PostgreSQL metadata database and a small Spark
standalone cluster, MinIO object storage and ClickHouse. Superset is the BI
interface connected to ClickHouse.

### Prerequisites

- Docker Desktop with the Linux engine enabled
- Docker Compose v2 (`docker compose`)

### Start Airflow

From the repository root:

```powershell
docker compose up -d
```

The initialization container migrates the Airflow database and creates the local
administrator automatically. Check the services with:

```powershell
docker compose ps
curl.exe http://localhost:8080/health
```

Open <http://localhost:8080> and log in with:

- Username: `admin`
- Password: `admin`

Stop the stack with:

```powershell
docker compose down
```

The PostgreSQL data is kept in the Docker volume `chicago-taxi-data-pipeline_postgres_data`.

### Run the Spark smoke test

After starting the stack, trigger the DAG from the Airflow UI or with:

```powershell
docker compose exec airflow-scheduler airflow dags trigger taxi_spark_smoke_test
```

The DAG submits `spark_jobs/smoke_test.py` from Airflow to the Spark standalone
master. The Spark master UI is available at <http://localhost:8081>.

### ClickHouse

ClickHouse is the SQL serving layer for BI. MinIO remains the official datalake
storage for Bronze, Silver and Gold Parquet data; ClickHouse will expose selected
Gold datasets for fast analytical queries.

- HTTP endpoint: <http://localhost:8123>
- Native protocol: `localhost:19000`
- Database: `analytics`
- Username: `default`
- Password: `clickhouse`

### Superset

Superset reads the analytical tables from ClickHouse. MinIO is not connected
directly to the BI tool; Spark writes the Gold Parquet data to MinIO and the
serving layer exposes it through ClickHouse.

- Interface: <http://localhost:8088>
- Username: `admin`
- Password: `admin`
- ClickHouse SQLAlchemy URI: `clickhousedb://default:clickhouse@clickhouse:8123/analytics`

The ClickHouse database connection is registered automatically by
`superset-datasource-init` after Superset becomes healthy.

### MinIO

MinIO provides the local S3-compatible object storage for the datalake. The
`datalake` bucket is created automatically by `minio-init`.

- Console: <http://localhost:9003>
- S3 endpoint: <http://localhost:9002>
- Username: `minioadmin`
- Password: `minioadmin`

Parquet is the storage format for the first implementation. Hudi is deferred
because this test does not require record-level upserts or time travel; adding it
now would increase the Spark and storage configuration without improving the
required Bronze/Silver/Gold pipeline.

### Run the Spark-to-MinIO smoke test

The `taxi_spark_smoke_test` DAG creates a small Parquet dataset with PySpark and
writes it directly to `s3a://datalake/smoke_test/` through MinIO's S3-compatible
endpoint:

```powershell
docker compose exec airflow-scheduler airflow tasks test taxi_spark_smoke_test run_spark_job 2026-09-10
docker compose run --rm --entrypoint sh minio-init -c "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null; mc ls --recursive local/datalake/smoke_test"
```

The Hadoop S3A dependencies are bundled in the Airflow image during
`docker compose build` under `/opt/spark-jars`. The DAG uses local JARs, so the
Spark task does not download Maven dependencies on every run.