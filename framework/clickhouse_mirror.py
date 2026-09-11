import os
import re
from typing import Any, Dict, List, Tuple

import requests
from minio import Minio

from framework.config import load_pipeline_config


def _safe_identifier(value: str) -> str:
    """Normalize a configured name into a ClickHouse identifier."""
    identifier = re.sub(r"[^0-9a-zA-Z_]+", "_", value).strip("_").lower()
    if not identifier:
        raise ValueError("Identifier cannot be empty")
    if identifier[0].isdigit():
        identifier = "_{}".format(identifier)
    return identifier


def _sql_identifier(value: str) -> str:
    """Quote a ClickHouse identifier after normalization."""
    return "`{}`".format(_safe_identifier(value))


def _clickhouse_query(sql: str) -> None:
    """Execute one SQL statement through the ClickHouse HTTP endpoint."""
    host = os.environ.get("CLICKHOUSE_HOST", "clickhouse")
    port = os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")
    user = os.environ.get("CLICKHOUSE_USER", "default")
    password = os.environ.get("CLICKHOUSE_PASSWORD", "clickhouse")
    response = requests.post(
        "http://{}:{}/".format(host, port),
        params={"user": user, "password": password},
        data=sql.encode("utf-8"),
        timeout=120,
    )
    if response.status_code >= 400:
        raise RuntimeError(
            "ClickHouse query failed with HTTP {}: {}\nSQL: {}".format(
                response.status_code, response.text.strip(), sql.strip()
            )
        )


def _minio_client() -> Minio:
    """Build the MinIO client used to check which datasets exist."""
    return Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )


def _has_parquet(bucket: str, prefix: str) -> bool:
    """Return true when a MinIO prefix contains at least one Parquet file."""
    normalized_prefix = prefix.strip("/") + "/"
    for item in _minio_client().list_objects(bucket, normalized_prefix, recursive=True):
        if item.object_name.endswith(".parquet"):
            return True
    return False


def _s3_url(bucket: str, prefix: str, pattern: str = "*.parquet") -> str:
    """Build the URL consumed by ClickHouse s3() from a MinIO prefix."""
    endpoint = os.environ.get("CLICKHOUSE_S3_ENDPOINT", "http://minio:9000")
    return "{}/{}/{}/{}".format(endpoint.rstrip("/"), bucket, prefix.strip("/"), pattern)


def _mirror_table(database: str, table: str, url: str) -> None:
    """Replace a ClickHouse table with an external S3-backed Parquet table."""
    database_sql = _sql_identifier(database)
    table_sql = _sql_identifier(table)
    access_key = os.environ["MINIO_ACCESS_KEY"]
    secret_key = os.environ["MINIO_SECRET_KEY"]
    _clickhouse_query("CREATE DATABASE IF NOT EXISTS {}".format(database_sql))
    _clickhouse_query("DROP TABLE IF EXISTS {}.{}".format(database_sql, table_sql))
    _clickhouse_query(
        """
        CREATE TABLE {}.{}
        ENGINE = S3('{}', '{}', '{}', 'Parquet')
        """.format(database_sql, table_sql, url, access_key, secret_key)
    )


def _silver_tables(config: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Build ClickHouse mirror definitions for Silver datasets."""
    pipeline = _safe_identifier(config["pipeline"]["name"])
    silver = config["silver"]
    tech = silver["tech"]
    return [
        ("silver", silver["raw"]["table_name"], silver["raw"]["prefix"]),
        ("silver", tech["table_name"], tech["prefix"]),
        ("silver", "reject_{}".format(pipeline), tech["prefix"].replace("/tech", "/reject")),
        ("silver", silver["func"]["table_name"], silver["func"]["prefix"]),
    ]


def _gold_tables(config: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Build ClickHouse mirror definitions for Gold dimensions and facts."""
    gold = config["gold"]
    tables = []
    for dimension in gold.get("dimensions", []):
        tables.append(
            (
                "gold",
                dimension["name"],
                "{}/{}".format(gold["prefix"], dimension.get("path", dimension["name"])),
            )
        )
    for fact in gold.get("facts", []):
        tables.append(
            (
                "gold",
                fact["name"],
                "{}/{}".format(gold["prefix"], fact.get("path", fact["name"])),
            )
        )
    return tables


def _monitoring_tables(config: Dict[str, Any]) -> List[Tuple[str, str, str]]:
    """Build ClickHouse mirror definitions for Monitoring datasets."""
    prefix = config["monitoring"]["prefix"]
    tables = [
        ("monitoring", "bronze_events", "{}/bronze".format(prefix)),
        ("monitoring", "raw_events", "{}/raw".format(prefix)),
        ("monitoring", "tech_events", "{}/tech".format(prefix)),
        ("monitoring", "func_events", "{}/func".format(prefix)),
    ]
    for dimension in config.get("gold", {}).get("dimensions", []):
        tables.append(
            (
                "monitoring",
                "gold_{}_events".format(dimension["name"]),
                "{}/gold/dimensions/{}".format(prefix, dimension["name"]),
            )
        )
    for fact in config.get("gold", {}).get("facts", []):
        tables.append(
            (
                "monitoring",
                "gold_{}_events".format(fact["name"]),
                "{}/gold/facts/{}".format(prefix, fact["name"]),
            )
        )
    return tables


def mirror_pipeline_to_clickhouse(config_path: str) -> List[str]:
    """Create ClickHouse silver, gold and monitoring mirrors from YAML config."""
    config = load_pipeline_config(config_path)
    for database in ("silver", "gold", "monitoring"):
        _clickhouse_query("CREATE DATABASE IF NOT EXISTS {}".format(_sql_identifier(database)))

    bucket_by_database = {
        "silver": config["bronze"]["bucket"],
        "gold": config["gold"]["bucket"],
        "monitoring": config["monitoring"]["bucket"],
    }
    table_definitions = _silver_tables(config) + _gold_tables(config) + _monitoring_tables(config)
    created_tables = []
    for database, table, prefix in table_definitions:
        bucket = bucket_by_database[database]
        if not _has_parquet(bucket, prefix):
            continue
        pattern = "ingestion_timestamp=*/*.parquet" if database == "silver" and "/func" not in prefix else "*.parquet"
        _mirror_table(database, table, _s3_url(bucket, prefix, pattern))
        created_tables.append("{}.{}".format(database, _safe_identifier(table)))
    print("ClickHouse mirrored tables: {}".format(", ".join(created_tables)))
    return created_tables
