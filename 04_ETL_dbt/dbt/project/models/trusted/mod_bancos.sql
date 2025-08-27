
{{ config(materialized='incremental', schema='trusted', unique_key=['cnpj', 'segmento', 'cnpj'],
    incremental_strategy='merge') }}

select * from {{ ref('stg_bancos') }}
{% if is_incremental() %}
  where data_atualizacao > (select max(data_atualizacao) from {{ this }})
{% endif %}