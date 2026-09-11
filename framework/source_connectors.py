import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from minio import Minio

from framework.config import load_pipeline_config


def _monitoring_event_data(config: Dict[str, Any], event: Dict[str, Any]) -> Dict[str, Any]:
    """Build a normalized monitoring event shared by all pipeline layers."""
    return {
        "pipeline": config["pipeline"]["name"],
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "layer": event["layer"],
        "dataset_name": event.get("dataset_name", ""),
        "dataset_type": event.get("dataset_type", ""),
        "status": event.get("status", "success"),
        "batch_id": event.get("batch_id", ""),
        "input_path": event.get("input_path", ""),
        "output_path": event.get("output_path", ""),
        "row_count": int(event.get("row_count", 0)),
        "rejected_count": int(event.get("rejected_count", 0)),
        "reject_path": event.get("reject_path", ""),
        "write_mode": event.get("write_mode", ""),
        "dedup_strategy": event.get("dedup_strategy", ""),
        "error_type": event.get("error_type", ""),
        "error_message": event.get("error_message", ""),
    }


def _write_monitoring_event(
    minio_client: Minio, config: Dict[str, Any], event: Dict[str, Any]
) -> None:
    """Write one Bronze monitoring event as Parquet from the Python task."""
    monitoring = config.get("monitoring", {})
    bucket = monitoring.get("bucket", config["bronze"]["bucket"])
    prefix = monitoring.get(
        "prefix", "{}/monitoring".format(config["pipeline"]["name"])
    )
    event_data = _monitoring_event_data(config, event)
    object_name = "{}/{}/{}.parquet".format(
        prefix,
        event_data["layer"],
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"),
    )
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as output:
        local_path = Path(output.name)
    try:
        table = pa.Table.from_pylist([event_data])
        pq.write_table(table, local_path)
        minio_client.fput_object(
            bucket, object_name, str(local_path), content_type="application/octet-stream"
        )
    finally:
        local_path.unlink(missing_ok=True)


def _build_api_params(
    source_config: Dict[str, Any], limit: int, offset: int
) -> Dict[str, Any]:
    """Build one paginated request for the configured API provider."""
    if source_config["provider"] != "soda":
        raise ValueError("Unsupported API provider: {}".format(source_config["provider"]))
    date_filter = source_config["filter"]
    return {
        "$where": "{} between '{}' and '{}'".format(
            date_filter["column"], date_filter["start"], date_filter["end"]
        ),
        "$limit": limit,
        "$offset": offset,
    }


def ingest_api_to_bronze_from_source(config_path: str) -> str:
    """Retrieve a configured API source and store raw JSONL records in Bronze."""
    config = load_pipeline_config(config_path)
    source = config["source"]
    bronze = config["bronze"]
    pagination = source["pagination"]
    page_size = int(pagination["page_size"])
    max_rows = int(pagination["max_rows"])
    request_timeout = int(pagination.get("request_timeout_seconds", 120))
    max_retries = int(pagination.get("max_retries", 3))
    if page_size <= 0 or max_rows <= 0:
        raise ValueError("page_size and max_rows must be positive")

    minio_client = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    bucket = bronze["bucket"]
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    object_name = "{}/ingestion_timestamp={}/data.jsonl".format(
        bronze["prefix"], timestamp
    )
    total_rows = 0
    offset = 0
    local_path = None
    _write_monitoring_event(
        minio_client,
        config,
        {
            "layer": "bronze",
            "status": "started",
            "batch_id": timestamp,
            "input_path": source["endpoint"],
            "output_path": "s3a://{}/{}".format(bucket, object_name),
        },
    )
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".jsonl", delete=False
        ) as output:
            local_path = Path(output.name)
            while total_rows < max_rows:
                params = _build_api_params(
                    source, min(page_size, max_rows - total_rows), offset
                )
                response = None
                for attempt in range(max_retries + 1):
                    try:
                        response = requests.get(
                            source["endpoint"], params=params, timeout=request_timeout
                        )
                        if response.status_code not in (429, 500, 502, 503, 504):
                            break
                    except requests.RequestException:
                        if attempt == max_retries:
                            raise
                    time.sleep(min(60, 2 ** attempt))
                if response is None:
                    raise RuntimeError("API request did not return a response")
                if response.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError("API request failed after retries")
                response.raise_for_status()
                rows = response.json()
                if not rows:
                    break
                for row in rows:
                    output.write(json.dumps(row, ensure_ascii=False) + "\n")
                row_count = len(rows)
                total_rows += row_count
                offset += row_count
                if row_count < page_size:
                    break
                time.sleep(1)

        minio_client.fput_object(
            bucket,
            object_name,
            str(local_path),
            content_type="application/x-ndjson",
        )
        status = "success" if total_rows else "no_data"
        _write_monitoring_event(
            minio_client,
            config,
            {
                "layer": "bronze",
                "status": status,
                "batch_id": timestamp,
                "input_path": source["endpoint"],
                "output_path": "s3a://{}/{}".format(bucket, object_name),
                "row_count": total_rows,
            },
        )
    except Exception as exc:
        _write_monitoring_event(
            minio_client,
            config,
            {
                "layer": "bronze",
                "status": "failed",
                "batch_id": timestamp,
                "input_path": source["endpoint"],
                "output_path": "s3a://{}/{}".format(bucket, object_name),
                "row_count": total_rows,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        raise
    finally:
        if local_path is not None:
            local_path.unlink(missing_ok=True)
    return object_name