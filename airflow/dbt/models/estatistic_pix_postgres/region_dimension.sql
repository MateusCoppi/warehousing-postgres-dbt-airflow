{{ config(materialized='table') }}

with regions as (
    select distinct 
        "REC_REGIAO"
    from 
        warehouse.estatisticas_pix
)

select 
    case 
        when "REC_REGIAO" = 'NORTE' then 1
        when "REC_REGIAO" = 'NORDESTE' then 2
        when "REC_REGIAO" = 'CENTRO-OESTE' then 3
        when "REC_REGIAO" = 'SUDESTE' then 4
        when "REC_REGIAO" = 'SUL' then 5
        else 0
    end as "id_region",
    "REC_REGIAO" as "name_region"
from 
    regions
