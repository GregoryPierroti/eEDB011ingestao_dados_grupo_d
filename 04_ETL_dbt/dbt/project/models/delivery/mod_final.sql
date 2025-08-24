{{ config(materialized='table', schema='delivery') }}

with bancos as (
    select
        cnpj,
        segmento,
        nome,
        nome_processed
    from {{ source('public_trusted', 'mod_bancos') }}
    where cnpj is not null and cnpj <> '0'
),
reclamacoes as (
    select
        ano,
        trimestre,
        categoria,
        tipo,
        cnpj_if,
        instituicao_financeira,
        indice,
        quantidade_de_reclamacoes_reguladas_procedentes,
        quantidade_de_reclamacoes_reguladas_outras,
        quantidade_de_reclamacoes_nao_reguladas,
        quantidade_total_de_reclamacoes,
        quantidade_total_de_clientes_ccs_e_scr,
        quantidade_de_clientes_ccs,
        quantidade_de_clientes_scr
    from {{ source('public_trusted', 'mod_reclamacoes') }}
    where cnpj_if is not null and cnpj_if <> '0'
),
empregados as (
    select
        nome_processed as nome_processed_empregado,
        employer_sk,
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
        segmento as segmento_empregado,
        match_percent,
        nome as nome_empregado
    from {{ source('public_trusted', 'mod_empregados') }}
),
join_br as (
    select
        b.*,
        r.*
    from bancos b
    inner join reclamacoes r on b.cnpj = r.cnpj_if
),
final as (
    select
        jb.*, 
        e.*
    from join_br jb
    inner join empregados e on jb.nome_processed = e.nome_processed_empregado
)
select * from final
