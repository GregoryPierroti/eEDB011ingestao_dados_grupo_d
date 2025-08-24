{{ config(
    materialized='incremental',
    schema='trusted',
    unique_key=['employer_sk'],
    incremental_strategy='merge',
    tags=['trusted']
) }}

select *
from {{ ref('int_empregados') }}
{% if is_incremental() %}
  where data_atualizacao > (select coalesce(max(data_atualizacao), '1900-01-01'::timestamp) from {{ this }})
{% endif %}
