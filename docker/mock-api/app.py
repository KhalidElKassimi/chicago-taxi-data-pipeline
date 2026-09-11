import json
import os
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, Query


app = FastAPI(title="Chicago Taxi SODA-compatible mock API")


def load_partition(path: str) -> List[Dict[str, Any]]:
    """Load the real JSONL partition used by the local API simulation."""
    partition_path = Path(path)
    with partition_path.open("r", encoding="utf-8") as partition_file:
        return [json.loads(line) for line in partition_file if line.strip()]


DATA = load_partition(os.environ["MOCK_DATA_FILE"])


@app.get("/health")
def health() -> Dict[str, str]:
    """Return the health status used by Docker Compose."""
    return {"status": "ok", "records": str(len(DATA))}


@app.get("/resource/wrvz-psew.json")
def taxi_trips(
    limit: int = Query(1000, alias="$limit", ge=1, le=5000),
    offset: int = Query(0, alias="$offset", ge=0),
    where: str | None = Query(None, alias="$where"),
) -> List[Dict[str, Any]]:
    """Return paginated taxi records using a small SODA-like query contract."""
    del where
    return DATA[offset : offset + limit]