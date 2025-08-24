{{ config(materialized='view', tags=['intermediate']) }}

{% if execute %}
  {% set less_cols = adapter.get_columns_in_relation(ref('stg_empregados_menor')) %}
  {% set v2_cols   = adapter.get_columns_in_relation(ref('stg_empregados_maior')) %}
  {% set less_colnames = less_cols | map(attribute='name') | map('lower') | list %}
  {% set v2_colnames   = v2_cols   | map(attribute='name') | map('lower') | list %}
{% else %}
  {% set less_colnames = [] %}
  {% set v2_colnames   = [] %}
{% endif %}

with less_v2 as (
  select
    {{ clean_text('employer_name') }}                as employer_name,
    cast(reviews_count   as integer)                 as reviews_count,
    cast(culture_count   as integer)                 as culture_count,
    cast(salaries_count  as integer)                 as salaries_count,
    cast(benefits_count  as integer)                 as benefits_count,

    {{ clean_text('employer_website') }}             as employer_website,
    {{ clean_text('employer_headquarters') }}        as employer_headquarters,
    cast(employer_founded as integer)                as employer_founded,
    {{ clean_text('employer_industry') }}            as employer_industry,
    {{ clean_text('employer_revenue') }}             as employer_revenue,
    {{ clean_text('url') }}                          as url,

    cast(geral                     as double precision)  as geral,
    cast(cultura_e_valores         as double precision)  as cultura_e_valores,
    cast(diversidade_e_inclusao    as double precision)  as diversidade_e_inclusao,
    cast(qualidade_de_vida         as double precision)  as qualidade_de_vida,
    cast(alta_lideranca            as double precision)  as alta_lideranca,
    cast(remuneracao_e_beneficios  as double precision)  as remuneracao_e_beneficios,
    cast(oportunidades_de_carreira as double precision)  as oportunidades_de_carreira,

    cast(recomendam_para_outras_pessoas  as integer)     as recomendam_para_outras_pessoas,
    cast(perspectiva_positiva_da_empresa as integer)     as perspectiva_positiva_da_empresa,

    {% if 'segmento' in less_colnames %}
      {{ clean_text('segmento') }}                      as segmento,
    {% else %}
      null::text                                        as segmento,
    {% endif %}

    {{ clean_text('nome') }}                            as nome,
    nullif(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              upper(coalesce({{ clean_text('nome') }}, '')),
              '\\b(S[\\.\\s\\/]*A|BANCO|LTDA)\\b',
              '',
              'g'
            ),
            '[^A-Z0-9 ]',
            ' ',
            'g'
          ),
          '\\s+',
          ' ',
          'g'
        )
      ),
      ''
    )                                         as nome_processed,
    cast(match_percent as integer)                      as match_percent,

    data_atualizacao,
    'glassdoor_consolidado_join_match_less_v2'          as _source_table
  from {{ ref('stg_empregados_menor') }}
),

v2 as (
  select
    {{ clean_text('employer_name') }}                as employer_name,
    cast(reviews_count   as integer)                 as reviews_count,
    cast(culture_count   as integer)                 as culture_count,
    cast(salaries_count  as integer)                 as salaries_count,
    cast(benefits_count  as integer)                 as benefits_count,

    {{ clean_text('employer_website') }}             as employer_website,
    {{ clean_text('employer_headquarters') }}        as employer_headquarters,
    cast(employer_founded as integer)                as employer_founded,
    {{ clean_text('employer_industry') }}            as employer_industry,
    {{ clean_text('employer_revenue') }}             as employer_revenue,
    {{ clean_text('url') }}                          as url,

    cast(geral                     as double precision)  as geral,
    cast(cultura_e_valores         as double precision)  as cultura_e_valores,
    cast(diversidade_e_inclusao    as double precision)  as diversidade_e_inclusao,
    cast(qualidade_de_vida         as double precision)  as qualidade_de_vida,
    cast(alta_lideranca            as double precision)  as alta_lideranca,
    cast(remuneracao_e_beneficios  as double precision)  as remuneracao_e_beneficios,
    cast(oportunidades_de_carreira as double precision)  as oportunidades_de_carreira,

    cast(recomendam_para_outras_pessoas  as integer)     as recomendam_para_outras_pessoas,
    cast(perspectiva_positiva_da_empresa as integer)     as perspectiva_positiva_da_empresa,

    {% if 'segmento' in v2_colnames %}
      {{ clean_text('segmento') }}                      as segmento,
    {% else %}
      null::text                                        as segmento,
    {% endif %}

    {{ clean_text('nome') }}                            as nome,
    nullif(
      trim(
        regexp_replace(
          regexp_replace(
            regexp_replace(
              upper(coalesce({{ clean_text('nome') }}, '')),
              '\\b(S[\\.\\s\\/]*A|BANCO|LTDA)\\b',
              '',
              'g'
            ),
            '[^A-Z0-9 ]',
            ' ',
            'g'
          ),
          '\\s+',
          ' ',
          'g'
        )
      ),
      ''
    )                                         as nome_processed,
    cast(match_percent as integer)                      as match_percent,

    data_atualizacao,
    'glassdoor_consolidado_join_match_v2'               as _source_table
  from {{ ref('stg_empregados_maior') }}
),

unioned as (
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
    nome_processed,
    match_percent,
    data_atualizacao,
    _source_table
  from less_v2
  union all
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
    nome_processed,
    match_percent,
    data_atualizacao,
    _source_table
  from v2
),

final as (
  select
    md5(
      coalesce(nome,'') || '|' ||
      coalesce(employer_name,'') || '|' ||
      coalesce(url,'') || '|' ||
      coalesce(segmento,'')
    ) as employer_sk,
    *
  from unioned
)

select * from final
