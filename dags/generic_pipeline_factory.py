import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from framework.clickhouse_mirror import mirror_pipeline_to_clickhouse
from framework.source_connectors import ingest_api_to_bronze_from_source


CONFIG_DIRECTORY = Path("/opt/airflow/config/pipelines")
SPARK_JARS = (
    "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/spark-jars/wildfly-openssl-1.0.7.Final.jar"
)
SPARK_CONF = {
    "spark.pyspark.python": "python3",
    "spark.executorEnv.PYSPARK_PYTHON": "python3",
    "spark.hadoop.fs.s3a.access.key": os.environ["MINIO_ACCESS_KEY"],
    "spark.hadoop.fs.s3a.secret.key": os.environ["MINIO_SECRET_KEY"],
    "spark.hadoop.fs.s3a.endpoint": os.environ.get("MINIO_S3A_ENDPOINT", "http://minio:9000"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.adaptive.enabled": "false",
}


def load_pipeline_config(config_path: Path) -> Dict[str, Any]:
    """Load one YAML configuration used to generate a pipeline DAG."""
    with config_path.open("r", encoding="utf-8") as config_file:
        return yaml.safe_load(config_file)


def build_pipeline_dag(config_path: Path, config: Dict[str, Any]) -> DAG:
    """Build one end-to-end DAG from one pipeline YAML configuration."""
    pipeline_name = config["pipeline"]["name"]
    with DAG(
        dag_id="pipeline_{}".format(pipeline_name),
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=[pipeline_name, "framework"],
    ) as dag:
        ingest = PythonOperator(
            task_id="ingest_api_to_bronze",
            python_callable=ingest_api_to_bronze_from_source,
            op_kwargs={"config_path": str(config_path)},
        )

        bronze_to_raw = SparkSubmitOperator(
            task_id="bronze_to_raw",
            application="/opt/airflow/spark_jobs/bronze_to_raw.py",
            application_args=["--config", str(config_path)],
            conn_id="spark_default",
            name="{}-bronze-to-raw".format(pipeline_name),
            jars=SPARK_JARS,
            conf=SPARK_CONF,
        )

        raw_to_tech = SparkSubmitOperator(
            task_id="raw_to_tech",
            application="/opt/airflow/spark_jobs/raw_to_tech.py",
            application_args=["--config", str(config_path)],
            conn_id="spark_default",
            name="{}-raw-to-tech".format(pipeline_name),
            jars=SPARK_JARS,
            conf=SPARK_CONF,
        )

        tech_to_func = SparkSubmitOperator(
            task_id="tech_to_func",
            application="/opt/airflow/spark_jobs/tech_to_func.py",
            application_args=["--config", str(config_path)],
            conn_id="spark_default",
            name="{}-tech-to-func".format(pipeline_name),
            jars=SPARK_JARS,
            conf=SPARK_CONF,
        )

        silver_to_gold = SparkSubmitOperator(
            task_id="silver_to_gold",
            application="/opt/airflow/spark_jobs/silver_to_gold.py",
            application_args=["--config", str(config_path)],
            conn_id="spark_default",
            name="{}-silver-to-gold".format(pipeline_name),
            jars=SPARK_JARS,
            conf=SPARK_CONF,
        )

        mirror_to_clickhouse = PythonOperator(
            task_id="mirror_to_clickhouse",
            python_callable=mirror_pipeline_to_clickhouse,
            op_kwargs={"config_path": str(config_path)},
        )

        ingest >> bronze_to_raw >> raw_to_tech >> tech_to_func >> silver_to_gold >> mirror_to_clickhouse

    return dag


for pipeline_config in CONFIG_DIRECTORY.glob("*.yml"):
    pipeline_config_data = load_pipeline_config(pipeline_config)
    generated_dag = build_pipeline_dag(pipeline_config, pipeline_config_data)
    globals()[generated_dag.dag_id] = generated_dag
