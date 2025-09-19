{{
  config(
    materialized='table',
    docs={'description': 'Reference table for municipalities with state and region information'}
  )
}}

select
    cast(codigo as integer) as municipio_codigo,
    trim(upper(descricao)) as municipio_nome,
    
    -- Extract state from municipality code (first 2 digits)
    case cast(substr(cast(codigo as varchar), 1, 2) as integer)
        when 11 then 'RO' when 12 then 'AC' when 13 then 'AM' when 14 then 'RR'
        when 15 then 'PA' when 16 then 'AP' when 17 then 'TO' when 21 then 'MA'
        when 22 then 'PI' when 23 then 'CE' when 24 then 'RN' when 25 then 'PB'
        when 26 then 'PE' when 27 then 'AL' when 28 then 'SE' when 29 then 'BA'
        when 31 then 'MG' when 32 then 'ES' when 33 then 'RJ' when 35 then 'SP'
        when 41 then 'PR' when 42 then 'SC' when 43 then 'RS' when 50 then 'MS'
        when 51 then 'MT' when 52 then 'GO' when 53 then 'DF'
        else 'EXTERIOR'
    end as uf,
    
    -- Region classification
    case cast(substr(cast(codigo as varchar), 1, 2) as integer)
        when 11, 12, 13, 14, 15, 16, 17 then 'NORTE'
        when 21, 22, 23, 24, 25, 26, 27, 28, 29 then 'NORDESTE'
        when 31, 32, 33, 35 then 'SUDESTE'
        when 41, 42, 43 then 'SUL'
        when 50, 51, 52, 53 then 'CENTRO_OESTE'
        else 'EXTERIOR'
    end as regiao,
    
    -- Capital city identification (simplified list)
    case codigo
        when '1100205' then true  -- Porto Velho
        when '1200401' then true  -- Rio Branco
        when '1302603' then true  -- Manaus
        when '1400100' then true  -- Boa Vista
        when '1501402' then true  -- Belém
        when '1600303' then true  -- Macapá
        when '1721000' then true  -- Palmas
        when '2111300' then true  -- São Luís
        when '2211001' then true  -- Teresina
        when '2304400' then true  -- Fortaleza
        when '2408102' then true  -- Natal
        when '2507507' then true  -- João Pessoa
        when '2611606' then true  -- Recife
        when '2704302' then true  -- Maceió
        when '2800308' then true  -- Aracaju
        when '2927408' then true  -- Salvador
        when '3106200' then true  -- Belo Horizonte
        when '3205309' then true  -- Vitória
        when '3304557' then true  -- Rio de Janeiro
        when '3550308' then true  -- São Paulo
        when '4106902' then true  -- Curitiba
        when '4205407' then true  -- Florianópolis
        when '4314902' then true  -- Porto Alegre
        when '5002704' then true  -- Campo Grande
        when '5103403' then true  -- Cuiabá
        when '5208707' then true  -- Goiânia
        when '5300108' then true  -- Brasília
        else false
    end as is_capital,
    
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'municipio') }}

where codigo is not null
  and codigo ~ '^[0-9]+$'