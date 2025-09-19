{{
  config(
    materialized='table',
    docs={'description': 'Reference table for CNAE economic activity codes with sector classification'}
  )
}}

select
    cast(codigo as varchar) as cnae_codigo,
    trim(upper(descricao)) as cnae_descricao,
    
    -- Extract sector classification from CNAE structure
    case 
        when cast(codigo as integer) between 01115 and 03220 then 'AGRICULTURA'
        when cast(codigo as integer) between 05002 and 09900 then 'EXTRACAO_MINERAL'
        when cast(codigo as integer) between 10112 and 33999 then 'INDUSTRIA'
        when cast(codigo as integer) between 35111 and 35999 then 'ENERGIA'
        when cast(codigo as integer) between 36001 and 39000 then 'AGUA_ESGOTO'
        when cast(codigo as integer) between 41107 and 43999 then 'CONSTRUCAO'
        when cast(codigo as integer) between 45111 and 47997 then 'COMERCIO'
        when cast(codigo as integer) between 49116 and 53202 then 'TRANSPORTE'
        when cast(codigo as integer) between 55108 and 56202 then 'HOSPEDAGEM_ALIMENTACAO'
        when cast(codigo as integer) between 58115 and 63919 then 'INFORMACAO_COMUNICACAO'
        when cast(codigo as integer) between 64110 and 66303 then 'FINANCEIRO'
        when cast(codigo as integer) between 68101 and 68220 then 'IMOBILIARIO'
        when cast(codigo as integer) between 69112 and 75000 then 'PROFISSIONAL_TECNICO'
        when cast(codigo as integer) between 77110 and 82999 then 'ADMINISTRATIVO'
        when cast(codigo as integer) between 84110 and 84302 then 'PUBLICO'
        when cast(codigo as integer) between 85112 and 85996 then 'EDUCACAO'
        when cast(codigo as integer) between 86101 and 88000 then 'SAUDE'
        when cast(codigo as integer) between 90015 and 93299 then 'CULTURA_LAZER'
        when cast(codigo as integer) between 94110 and 96099 then 'OUTROS_SERVICOS'
        when cast(codigo as integer) between 97001 and 97700 then 'DOMESTICO'
        when cast(codigo as integer) between 99001 and 99999 then 'ORGANISMOS_INTERNACIONAIS'
        else 'NAO_CLASSIFICADO'
    end as setor_economico,
    
    -- Technology sector identification for G2Market
    case 
        when descricao ilike '%software%' 
            or descricao ilike '%tecnologia%'
            or descricao ilike '%informática%'
            or descricao ilike '%programação%'
            or descricao ilike '%desenvolvimento%'
            or descricao ilike '%dados%'
            or descricao ilike '%digital%'
            or descricao ilike '%internet%'
        then true
        else false
    end as is_tech_sector,
    
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'cnae') }}

where codigo is not null
  and codigo ~ '^[0-9]+$'