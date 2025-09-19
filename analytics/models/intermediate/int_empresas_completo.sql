{{
  config(
    materialized='view',
    docs={'description': 'Intermediate model combining company and establishment data for comprehensive business analysis'}
  )
}}

select
    e.cnpj_completo,
    e.cnpj_basico,
    
    -- Company basic info
    emp.razao_social_clean,
    emp.capital_social_valor,
    emp.porte_empresa_desc,
    emp.natureza_juridica_codigo,
    
    -- Establishment info
    e.tipo_estabelecimento,
    e.nome_fantasia_clean,
    e.situacao_cadastral_desc,
    e.data_situacao_cadastral,
    e.data_inicio_atividade,
    
    -- Economic activity
    e.cnae_principal_codigo,
    cnae.cnae_descricao as atividade_principal,
    cnae.setor_economico,
    cnae.is_tech_sector,
    
    -- Geography
    e.uf,
    e.codigo_municipio,
    mun.municipio_nome,
    mun.regiao,
    mun.is_capital,
    
    -- Tax regime
    rt.regime_tributario_atual,
    rt.tem_opcao_simples,
    rt.tem_opcao_mei,
    
    -- Contact
    e.email_clean,
    e.telefone1_formatado,
    
    -- Business metrics
    case 
        when e.data_inicio_atividade is not null 
        then date_diff('year', e.data_inicio_atividade, current_date)
        else null
    end as anos_funcionamento,
    
    case 
        when e.situacao_cadastral_desc = 'ATIVA' then true
        else false
    end as is_empresa_ativa,
    
    case 
        when emp.capital_social_valor >= {{ var('min_company_capital') }} then true
        else false
    end as is_capital_significativo,
    
    current_date as analysis_date

from {{ ref('stg_estabelecimentos') }} e
inner join {{ ref('stg_empresas') }} emp
    on e.cnpj_basico = emp.cnpj_basico
left join {{ ref('stg_cnae') }} cnae
    on e.cnae_principal_codigo = cnae.cnae_codigo
left join {{ ref('stg_municipios') }} mun
    on e.codigo_municipio = mun.municipio_codigo
left join {{ ref('stg_regime_tributario') }} rt
    on e.cnpj_basico = rt.cnpj_basico

-- Focus on headquarters and active companies for cleaner analysis
where e.tipo_estabelecimento = 'MATRIZ'
  and e.situacao_cadastral_desc in ('ATIVA', 'SUSPENSA')