{{ config(materialized='table') }}

with estatistic_pix_fact as (
    select
        "id",
        "AnoMes" as "Year Month",
        "PAG_PFPJ" as "Payment PFPJ",
        "REC_PFPJ" as "Receiver PFPJ",
        case 
            when "PAG_REGIAO" = 'NORTE' then 1
            when "PAG_REGIAO" = 'NORDESTE' then 2
            when "PAG_REGIAO" = 'CENTRO-OESTE' then 3
            when "PAG_REGIAO" = 'SUDESTE' then 4
            when "PAG_REGIAO" = 'SUL' then 5
            else 0
        end as "Payment Region",
        case 
            when "REC_REGIAO" = 'NORTE' then 1
            when "REC_REGIAO" = 'NORDESTE' then 2
            when "REC_REGIAO" = 'CENTRO-OESTE' then 3
            when "REC_REGIAO" = 'SUDESTE' then 4
            when "REC_REGIAO" = 'SUL' then 5
            else 0
        end as "Receiver Region",
        "PAG_IDADE" as "Age Payment",
        "REC_IDADE" AS "Age Receiver",
        "VALOR" as "Value",
        "QUANTIDADE" "Quantity"
    from 
        warehouse.estatisticas_pix
)

    select * from estatistic_pix_fact
