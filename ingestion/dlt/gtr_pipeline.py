from __future__ import annotations

import argparse
import datetime as dt
import os
from typing import Any, Dict, Iterable, Iterator, List

import dlt
import requests

DEFAULT_BASE_URL = os.getenv("GTR_API_BASE_URL", "https://gtr.ukri.org/gtr/api")
DEFAULT_PAGE_SIZE = int(os.getenv("GTR_API_PAGE_SIZE", "100"))
DEFAULT_TIMEOUT = int(os.getenv("GTR_API_TIMEOUT", "30"))
DEFAULT_WAREHOUSE_ROOT = os.getenv(
    "WAREHOUSE_ROOT", "/opt/local_data_platform/.local_data_platform/warehouse"
)


def _unwrap_items(payload: Dict[str, Any], singular_name: str) -> List[Dict[str, Any]]:
    plural_name = f"{singular_name}s"
    container = payload.get(plural_name, payload)
    if isinstance(container, dict):
        item = container.get(singular_name)
        if isinstance(item, list):
            return [x for x in item if isinstance(x, dict)]
        if isinstance(item, dict):
            return [item]
    if isinstance(container, list):
        return [x for x in container if isinstance(x, dict)]
    return []


def _fetch_collection(resource: str, singular_name: str) -> Iterator[Dict[str, Any]]:
    offset = 0
    while True:
        response = requests.get(
            f"{DEFAULT_BASE_URL}/{resource}",
            params={"s": offset, "size": DEFAULT_PAGE_SIZE},
            headers={"Accept": "application/json"},
            timeout=DEFAULT_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        items = _unwrap_items(payload, singular_name)
        if not items:
            break

        ingested_at = dt.datetime.now(dt.timezone.utc).isoformat()
        for item in items:
            yield {
                f"{singular_name}_id": item.get("id") or item.get("href") or "",
                "ingested_at": ingested_at,
                "payload": item,
            }

        if len(items) < DEFAULT_PAGE_SIZE:
            break
        offset += DEFAULT_PAGE_SIZE


@dlt.resource(name="bronze_gtr_projects_raw", write_disposition="replace")
def projects_resource() -> Iterable[Dict[str, Any]]:
    return _fetch_collection("projects", "project")


@dlt.resource(name="bronze_gtr_organizations_raw", write_disposition="replace")
def organizations_resource() -> Iterable[Dict[str, Any]]:
    return _fetch_collection("organisations", "organization")


def run_pipeline() -> None:
    os.makedirs(DEFAULT_WAREHOUSE_ROOT, exist_ok=True)
    os.environ.setdefault(
        "DESTINATION__FILESYSTEM__BUCKET_URL", f"file://{DEFAULT_WAREHOUSE_ROOT}"
    )

    pipeline = dlt.pipeline(
        pipeline_name="gtr_projects_orgs",
        destination="filesystem",
        dataset_name="gtr",
    )

    result = pipeline.run(
        [projects_resource(), organizations_resource()],
        loader_file_format="parquet",
        table_format="iceberg",
    )
    print(result)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", action="store_true", default=True)
    parser.parse_args()
    run_pipeline()
