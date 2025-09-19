{{
  config(
    materialized='table',
    docs={'description': 'Core fact table with comprehensive company information for general analytics'}
  )
}}

select
    ec.cnpj_completo,
    ec.cnpj_basico,
    
    -- Company identification
    ec.razao_social_clean as razao_social,
    ec.nome_fantasia_clean as nome_fantasia,
    
    -- Business classification
    ec.setor_economico,
    ec.atividade_principal,
    ec.is_tech_sector,
    ec.porte_empresa_desc as porte_empresa,
    ec.natureza_juridica_codigo,
    
    -- Financial
    ec.capital_social_valor,
    ec.is_capital_significativo,
    
    -- Geographic
    ec.regiao,
    ec.uf,
    ec.municipio_nome,
    ec.is_capital as empresa_em_capital,
    
    -- Status and dates
    ec.is_empresa_ativa,
    ec.situacao_cadastral_desc as situacao_cadastral,
    ec.data_situacao_cadastral,
    ec.data_inicio_atividade,
    ec.anos_funcionamento,
    
    -- Tax regime
    ec.regime_tributario_atual,
    ec.tem_opcao_simples,
    ec.tem_opcao_mei,
    
    -- Partnership structure
    coalesce(es.total_socios, 0) as total_socios,
    coalesce(es.complexidade_societaria, 'NAO_INFORMADO') as complexidade_societaria,
    coalesce(es.tem_participacao_estrangeira, false) as tem_participacao_estrangeira,
    coalesce(es.tem_participacao_corporativa, false) as tem_participacao_corporativa,
    
    -- Contact information
    ec.email_clean as email,
    ec.telefone1_formatado as telefone_principal,
    
    -- Derived metrics
    case 
        when ec.anos_funcionamento is null then 'NAO_INFORMADO'
        when ec.anos_funcionamento < 2 then 'STARTUP'
        when ec.anos_funcionamento between 2 and 5 then 'CRESCIMENTO'
        when ec.anos_funcionamento between 6 and 15 then 'CONSOLIDADA'
        else 'MADURA'
    end as fase_empresa,
    
    case 
        when ec.capital_social_valor = 0 then 'SEM_CAPITAL'
        when ec.capital_social_valor <= 10000 then 'MICRO_CAPITAL'
        when ec.capital_social_valor <= 100000 then 'PEQUENO_CAPITAL'
        when ec.capital_social_valor <= 1000000 then 'MEDIO_CAPITAL'
        else 'GRANDE_CAPITAL'
    end as faixa_capital,
    
    ec.analysis_date as data_processamento

from {{ ref('int_empresas_completo') }} ec
left join {{ ref('int_estrutura_societaria') }} es
    on ec.cnpj_basico = es.cnpj_basico