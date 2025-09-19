{{
  config(
    materialized='view',
    docs={'description': 'Staged establishment data with geocoding and business activity standardization'}
  )
}}

select
    cnpj_basico,
    cnpj_ordem,
    cnpj_dv,
    
    -- Build full CNPJ
    concat(cnpj_basico, cnpj_ordem, cnpj_dv) as cnpj_completo,
    
    -- Establishment type
    case identificador_matriz_filial
        when '1' then 'MATRIZ'
        when '2' then 'FILIAL'
        else 'NAO_INFORMADO'
    end as tipo_estabelecimento,
    identificador_matriz_filial as tipo_estabelecimento_codigo,
    
    -- Clean names
    trim(upper(nome_fantasia)) as nome_fantasia_clean,
    nome_fantasia as nome_fantasia_raw,
    
    -- Registration status
    case situacao_cadastral
        when '01' then 'NULA'
        when '02' then 'ATIVA'
        when '03' then 'SUSPENSA'
        when '04' then 'INAPTA'
        when '08' then 'BAIXADA'
        else 'OUTROS'
    end as situacao_cadastral_desc,
    situacao_cadastral as situacao_cadastral_codigo,
    
    -- Parse dates
    case 
        when data_situacao_cadastral is not null and data_situacao_cadastral != '0'
        then try_cast(data_situacao_cadastral as date)
        else null
    end as data_situacao_cadastral,
    
    case 
        when data_inicio_atividade is not null and data_inicio_atividade != '0'
        then try_cast(data_inicio_atividade as date)
        else null
    end as data_inicio_atividade,
    
    cast(motivo_situacao_cadastral as integer) as motivo_situacao_cadastral_codigo,
    
    -- Economic activity
    cnae_principal as cnae_principal_codigo,
    cnaes_secundarios,
    
    -- Geography
    upper(uf) as uf,
    cast(codigo_municipio as integer) as codigo_municipio,
    
    -- Contact
    lower(trim(correio_eletronico)) as email_clean,
    correio_eletronico as email_raw,
    
    -- Address fields (basic cleaning)
    trim(upper(tipo_logradouro)) as tipo_logradouro,
    trim(upper(logradouro)) as logradouro,
    numero,
    trim(upper(complemento)) as complemento,
    trim(upper(bairro)) as bairro,
    cep,
    
    -- Phone numbers
    concat_ws('-', ddd1, telefone1) as telefone1_formatado,
    concat_ws('-', ddd2, telefone2) as telefone2_formatado,
    concat_ws('-', ddd_fax, fax) as fax_formatado,
    
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'estabelecimento') }}

-- Data quality filters
where cnpj_basico is not null
  and length(cnpj_basico) = 8
  and cnpj_basico ~ '^[0-9]+$'
  and length(cnpj_ordem) = 4
  and length(cnpj_dv) = 2