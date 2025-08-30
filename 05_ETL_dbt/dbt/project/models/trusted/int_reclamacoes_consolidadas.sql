{{ config(materialized='view', tags=['intermediate','reclamacoes']) }}
select * from {{ ref('stg_reclamacoes_2021_tri_01') }}
union all
select * from {{ ref('stg_reclamacoes_2021_tri_02') }}
union all
select * from {{ ref('stg_reclamacoes_2021_tri_03') }}
union all
select * from {{ ref('stg_reclamacoes_2021_tri_04') }}
union all
select * from {{ ref('stg_reclamacoes_2022_tri_01') }}
union all
select * from {{ ref('stg_reclamacoes_2022_tri_03') }}
union all
select * from {{ ref('stg_reclamacoes_2022_tri_04') }}