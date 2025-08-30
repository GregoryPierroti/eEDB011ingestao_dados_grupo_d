{{ config(materialized='view', tags=['staging','reclamacoes']) }}
select 
*, 
'2021_tri_01' as fonte_tabela
from {{ source('reclamacoes','2021_tri_01') }}