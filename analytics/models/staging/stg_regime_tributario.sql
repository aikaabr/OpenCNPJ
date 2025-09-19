{{
  config(
    materialized='view',
    docs={'description': 'Staged tax regime data for Simples Nacional and MEI programs'}
  )
}}

select
    cnpj_basico,
    
    -- Simples Nacional
    case upper(opcao_simples)
        when 'S' then true
        when 'N' then false
        else null
    end as tem_opcao_simples,
    opcao_simples as opcao_simples_raw,
    
    case 
        when data_opcao_simples is not null and data_opcao_simples != '0'
        then try_cast(data_opcao_simples as date)
        else null
    end as data_opcao_simples,
    
    case 
        when data_exclusao_simples is not null and data_exclusao_simples != '0'
        then try_cast(data_exclusao_simples as date)
        else null
    end as data_exclusao_simples,
    
    -- MEI (Microempreendedor Individual)
    case upper(opcao_mei)
        when 'S' then true
        when 'N' then false
        else null
    end as tem_opcao_mei,
    opcao_mei as opcao_mei_raw,
    
    case 
        when data_opcao_mei is not null and data_opcao_mei != '0'
        then try_cast(data_opcao_mei as date)
        else null
    end as data_opcao_mei,
    
    case 
        when data_exclusao_mei is not null and data_exclusao_mei != '0'
        then try_cast(data_exclusao_mei as date)
        else null
    end as data_exclusao_mei,
    
    -- Current status derivation
    case 
        when upper(opcao_simples) = 'S' and (data_exclusao_simples is null or data_exclusao_simples = '0')
        then 'SIMPLES_ATIVO'
        when upper(opcao_mei) = 'S' and (data_exclusao_mei is null or data_exclusao_mei = '0')
        then 'MEI_ATIVO'
        when upper(opcao_simples) = 'S' and data_exclusao_simples is not null and data_exclusao_simples != '0'
        then 'SIMPLES_EXCLUIDO'
        when upper(opcao_mei) = 'S' and data_exclusao_mei is not null and data_exclusao_mei != '0'
        then 'MEI_EXCLUIDO'
        else 'REGIME_NORMAL'
    end as regime_tributario_atual,
    
    current_timestamp as processed_at

from {{ source('opencnpj_raw', 'simples') }}

-- Data quality filters
where cnpj_basico is not null
  and length(cnpj_basico) = 8
  and cnpj_basico ~ '^[0-9]+$'