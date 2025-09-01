{{ config(materialized='view', tags=['staging']) }}

with stg as (
  select
    employer_name,
    reviews_count,
    culture_count,
    salaries_count,
    benefits_count,
    employer_website,
    employer_headquarters,
    employer_founded,
    employer_industry,
    employer_revenue,
    url,
    geral,
    cultura_e_valores,
    diversidade_e_inclusao,
    qualidade_de_vida,
    alta_lideranca,
    remuneracao_e_beneficios,
    oportunidades_de_carreira,
    recomendam_para_outras_pessoas,
    perspectiva_positiva_da_empresa,
    segmento,
    nome,
    match_percent,
    current_timestamp as data_atualizacao
  from {{ source('empregados','glassdoor_consolidado_join_match_v2') }}
)
select * from stg
