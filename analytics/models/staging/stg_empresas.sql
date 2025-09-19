{{
  config(
    materialized='view',
    docs={'description': 'Staged company data with standardized formatting and data quality improvements'}
  )
}}

select
    cnpj_basico,
    trim(upper(razao_social)) as razao_social_clean,
    razao_social as razao_social_raw,
    
    -- Convert and validate numeric fields
    cast(natureza_juridica as integer) as natureza_juridica_codigo,
    cast(qualificacao_responsavel as integer) as qualificacao_responsavel_codigo,
    
    -- Clean and parse capital social
    case 
        when capital_social is null or capital_social = '' then 0.0
        when capital_social = '0' then 0.0
        else cast(replace(capital_social, ',', '.') as decimal(18,2))
    end as capital_social_valor,
    
    -- Standardize company size
    case 
        when porte_empresa = '00' then 'NAO_INFORMADO'
        when porte_empresa = '01' then 'MICRO_EMPRESA'
        when porte_empresa = '03' then 'PEQUENA_EMPRESA'
        when porte_empresa = '05' then 'DEMAIS_EMPRESAS'
        else 'OUTROS'
    end as porte_empresa_desc,
    porte_empresa as porte_empresa_codigo,
    
    cast(ente_federativo as integer) as ente_federativo_codigo,
    
    -- Add metadata
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'empresa') }}

-- Data quality filters
where cnpj_basico is not null
  and length(cnpj_basico) = 8
  and cnpj_basico ~ '^[0-9]+$'  -- Only numeric CNPJs