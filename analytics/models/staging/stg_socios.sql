{{
  config(
    materialized='view',
    docs={'description': 'Staged partner data with standardized partner types and ownership structure'}
  )
}}

select
    cnpj_basico,
    
    -- Partner type classification
    case identificador_socio
        when '1' then 'PESSOA_JURIDICA'
        when '2' then 'PESSOA_FISICA'
        when '3' then 'ESTRANGEIRO'
        else 'OUTROS'
    end as tipo_socio,
    identificador_socio as tipo_socio_codigo,
    
    -- Clean partner name
    trim(upper(nome_socio)) as nome_socio_clean,
    nome_socio as nome_socio_raw,
    
    -- Partner document (CNPJ/CPF)
    case 
        when cnpj_cpf_socio is not null and cnpj_cpf_socio != ''
        then trim(cnpj_cpf_socio)
        else null
    end as documento_socio,
    
    -- Determine document type based on length
    case 
        when length(trim(cnpj_cpf_socio)) = 11 then 'CPF'
        when length(trim(cnpj_cpf_socio)) = 14 then 'CNPJ'
        when cnpj_cpf_socio is null or cnpj_cpf_socio = '' then 'NAO_INFORMADO'
        else 'INVALIDO'
    end as tipo_documento_socio,
    
    cast(qualificacao_socio as integer) as qualificacao_socio_codigo,
    
    -- Parse entry date
    case 
        when data_entrada_sociedade is not null and data_entrada_sociedade != '0'
        then try_cast(data_entrada_sociedade as date)
        else null
    end as data_entrada_sociedade,
    
    -- Country code for foreign partners
    cast(codigo_pais as integer) as codigo_pais,
    
    -- Legal representative information
    trim(upper(representante_legal)) as representante_legal_clean,
    representante_legal as representante_legal_raw,
    trim(upper(nome_representante)) as nome_representante_clean,
    nome_representante as nome_representante_raw,
    cast(qualificacao_representante as integer) as qualificacao_representante_codigo,
    
    -- Age group
    case faixa_etaria
        when '1' then '0_12_ANOS'
        when '2' then '13_20_ANOS'
        when '3' then '21_30_ANOS'
        when '4' then '31_40_ANOS'
        when '5' then '41_50_ANOS'
        when '6' then '51_80_ANOS'
        when '8' then 'MAIS_80_ANOS'
        when '0' then 'NAO_SE_APLICA'
        else 'NAO_INFORMADO'
    end as faixa_etaria_desc,
    faixa_etaria as faixa_etaria_codigo,
    
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'socio') }}

-- Data quality filters
where cnpj_basico is not null
  and length(cnpj_basico) = 8
  and cnpj_basico ~ '^[0-9]+$'