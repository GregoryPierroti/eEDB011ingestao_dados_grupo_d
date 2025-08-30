with stg_bancos as (
    select
        segmento,
        nome,
        nullif(
            trim(
              regexp_replace(                                   -- 3) normaliza espaços
                regexp_replace(                                 -- 2) troca tudo que não for A–Z/0–9 por espaço
                  regexp_replace(                               -- 1) remove BANCO (no início), LTDA, S.A./S A, e PRUDENCIAL (com/sem hífen)
                    upper(coalesce({{ clean_text('nome') }}, '')),
                    '(^\s*\yBANCO\y\s*|\yLTDA\y\s*|\yS[\.\/\s]*A\y\s*|[-\s]*\yPRUDENCIAL\y\s*)',
                    ' ',
                    'g'
                  ),
                  '[^A-Z0-9]+',
                  ' ',
                  'g'
                ),
                '\s+',
                ' ',
                'g'
              )
            ),
            ''
        ) as nome_processed,

        cast(cnpj as text) as cnpj,
        current_timestamp as data_atualizacao
    from {{ source('bancos', 'EnquadramentoInicia_v2') }}
)

select * from stg_bancos
