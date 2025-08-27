{{ config(materialized='view', tags=['staging','reclamacoes']) }}
select *,  '2022_tri_03' as fonte_tabela
from {{ source('reclamacoes','2022_tri_03') }}