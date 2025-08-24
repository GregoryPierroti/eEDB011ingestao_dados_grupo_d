with stg_bancos as (
    select
        segmento,
        nome,
        cast(cnpj as text) as cnpj,
        current_timestamp as data_atualizacao
    from {{ source('bancos', 'EnquadramentoInicia_v2') }}
)

select * from stg_bancos