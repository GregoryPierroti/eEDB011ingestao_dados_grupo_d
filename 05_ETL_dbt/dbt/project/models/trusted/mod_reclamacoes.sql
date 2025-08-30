{{ config(materialized='table', schema='trusted', tags=['mart','reclamacoes']) }}
select * from {{ ref('int_reclamacoes_consolidadas') }}