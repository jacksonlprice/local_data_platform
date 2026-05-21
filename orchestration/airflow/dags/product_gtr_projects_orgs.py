from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ENV = {
    "WAREHOUSE_ROOT": "/opt/local_data_platform/.local_data_platform/warehouse",
    "DESTINATION__FILESYSTEM__BUCKET_URL": "file:///opt/local_data_platform/.local_data_platform/warehouse",
    "DBT_PROFILES_DIR": "/opt/local_data_platform/transformation/dbt",
    "OPENLINEAGE_URL": "http://marquez:5000",
    "OPENLINEAGE_NAMESPACE": "local_data_platform",
}

with DAG(
    dag_id="product_gtr_projects_orgs",
    description="Thin-slice GtR data product (projects + organizations)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["product", "gtr", "dlt", "dbt"],
) as dag:
    ingest_gtr = BashOperator(
        task_id="ingest_gtr",
        env=DEFAULT_ENV,
        cwd="/opt/local_data_platform",
        bash_command="python /opt/local_data_platform/ingestion/dlt/gtr_pipeline.py",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        env=DEFAULT_ENV,
        cwd="/opt/local_data_platform/transformation/dbt",
        bash_command=(
            "openlineage-inject dbt deps --project-dir . --profiles-dir . --target dev && "
            "openlineage-inject dbt build --project-dir . --profiles-dir . --target dev"
        ),
    )

    ingest_gtr >> dbt_build
