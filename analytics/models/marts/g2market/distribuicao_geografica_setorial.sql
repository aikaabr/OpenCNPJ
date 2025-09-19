{{
  config(
    materialized='table',
    docs={'description': 'Geographic distribution analysis of companies for G2Market strategy'}
  )
}}

select
    regiao,
    uf,
    municipio_nome,
    is_capital,
    setor_economico,
    
    -- Company counts
    count(*) as total_empresas,
    count(case when is_empresa_ativa then 1 end) as empresas_ativas,
    count(case when porte_empresa_desc = 'MICRO_EMPRESA' then 1 end) as micro_empresas,
    count(case when porte_empresa_desc = 'PEQUENA_EMPRESA' then 1 end) as pequenas_empresas,
    count(case when porte_empresa_desc = 'DEMAIS_EMPRESAS' then 1 end) as demais_empresas,
    
    -- Technology sector focus
    count(case when is_tech_sector then 1 end) as empresas_tech,
    round(
        count(case when is_tech_sector then 1 end) * 100.0 / count(*), 2
    ) as percentual_tech,
    
    -- Capital analysis
    count(case when is_capital_significativo then 1 end) as empresas_capital_alto,
    sum(capital_social_valor) as capital_social_total,
    avg(capital_social_valor) as capital_social_medio,
    median(capital_social_valor) as capital_social_mediano,
    
    -- Business maturity
    avg(anos_funcionamento) as media_anos_funcionamento,
    count(case when anos_funcionamento >= 5 then 1 end) as empresas_consolidadas,
    
    -- Tax regime distribution
    count(case when regime_tributario_atual = 'SIMPLES_ATIVO' then 1 end) as empresas_simples,
    count(case when regime_tributario_atual = 'MEI_ATIVO' then 1 end) as empresas_mei,
    count(case when regime_tributario_atual = 'REGIME_NORMAL' then 1 end) as empresas_regime_normal,
    
    -- Contact availability (for outreach)
    count(case when email_clean is not null and email_clean != '' then 1 end) as empresas_com_email,
    count(case when telefone1_formatado is not null and telefone1_formatado != '' then 1 end) as empresas_com_telefone,
    
    -- Market concentration (HHI-like metric)
    round(
        sum(power(count(*) * 100.0 / sum(count(*)) over(), 2)), 2
    ) as indice_concentracao,
    
    current_date as analysis_date

from {{ ref('int_empresas_completo') }}

where is_empresa_ativa = true

group by 
    regiao, uf, municipio_nome, is_capital, setor_economico

having count(*) >= 10  -- Filter out very small markets

order by 
    regiao, uf, total_empresas desc