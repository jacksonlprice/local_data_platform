with source as (
    select
        organization_id,
        cast(ingested_at as timestamp) as ingested_at,
        payload,
        to_json(payload) as payload_json
    from read_parquet('{{ env_var("WAREHOUSE_ROOT") }}/gtr/bronze_gtr_organizations_raw/**/*.parquet')
)

select
    organization_id,
    ingested_at,
    cast(json_extract_string(payload_json, '$.name') as varchar) as name,
    cast(json_extract_string(payload_json, '$.href') as varchar) as href,
    payload
from source
where nullif(organization_id, '') is not null
