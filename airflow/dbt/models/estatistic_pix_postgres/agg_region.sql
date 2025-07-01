{{ config(materialized='table') }}


with agg_region as (
    select
        "REC_REGIAO" as "Region",
        sum("VALOR") as "Value",
        sum("QUANTIDADE") as "Quantity"
    from 
        warehouse.estatisticas_pix
    group by 
        "REC_REGIAO"
)
select * from agg_region