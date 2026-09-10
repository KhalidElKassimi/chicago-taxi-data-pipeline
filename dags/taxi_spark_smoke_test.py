from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


with DAG(
    dag_id="taxi_spark_smoke_test",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "infrastructure"],
) as dag:
    start = EmptyOperator(task_id="start")

    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/spark_jobs/minio_smoke_test.py",
        conn_id="spark_default",
        name="taxi-spark-smoke-test",
        jars=(
            "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
            "/opt/spark-jars/wildfly-openssl-1.0.7.Final.jar"
        ),
        conf={
            "spark.pyspark.python": "python3",
            "spark.executorEnv.PYSPARK_PYTHON": "python3",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        },
        verbose=True,
    )

    finish = EmptyOperator(task_id="finish")

    start >> run_spark_job >> finish