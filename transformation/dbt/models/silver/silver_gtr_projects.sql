with source as (
    select
        project_id,
        cast(ingested_at as timestamp) as ingested_at,
        payload,
        to_json(payload) as payload_json
    from read_parquet('{{ env_var("WAREHOUSE_ROOT") }}/gtr/bronze_gtr_projects_raw/**/*.parquet')
)

select
    project_id,
    ingested_at,
    cast(json_extract_string(payload_json, '$.title') as varchar) as title,
    cast(json_extract_string(payload_json, '$.href') as varchar) as href,
    payload
from source
where nullif(project_id, '') is not null
