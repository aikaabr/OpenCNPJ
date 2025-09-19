{{
  config(
    materialized='table',
    docs={'description': 'Key performance indicators and trends for G2Market strategy dashboard'}
  )
}}

with kpis_base as (
    select
        -- Time dimension
        extract(year from data_inicio_atividade) as ano_fundacao,
        extract(quarter from current_date) as quarter_atual,
        
        -- Geographic dimension
        regiao,
        uf,
        
        -- Business dimensions
        setor_economico,
        porte_empresa_desc,
        regime_tributario_atual,
        
        -- Metrics
        count(*) as total_empresas,
        count(case when is_empresa_ativa then 1 end) as empresas_ativas,
        sum(capital_social_valor) as capital_total,
        avg(capital_social_valor) as capital_medio,
        count(case when is_tech_sector then 1 end) as empresas_tech,
        count(case when email_clean is not null then 1 end) as empresas_com_email,
        avg(anos_funcionamento) as media_anos_funcionamento
        
    from {{ ref('int_empresas_completo') }}
    where data_inicio_atividade >= '2015-01-01'  -- Focus on recent companies
    group by 1,2,3,4,5,6,7
)

select
    -- Summary KPIs
    'BRASIL' as geografia,
    'GERAL' as segmento,
    sum(empresas_ativas) as total_empresas_ativas,
    round(sum(capital_total) / 1000000000, 2) as capital_total_bilhoes,
    round(avg(capital_medio), 2) as capital_medio_nacional,
    
    -- Growth indicators
    sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) as empresas_novas_covid,
    round(
        sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) * 100.0 / sum(empresas_ativas), 2
    ) as percentual_empresas_novas,
    
    -- Technology adoption
    sum(empresas_tech) as total_empresas_tech,
    round(sum(empresas_tech) * 100.0 / sum(empresas_ativas), 2) as penetracao_tech_nacional,
    
    -- Digital readiness
    sum(empresas_com_email) as empresas_digitais,
    round(sum(empresas_com_email) * 100.0 / sum(empresas_ativas), 2) as maturidade_digital,
    
    -- Regional distribution
    count(distinct regiao) as regioes_ativas,
    count(distinct uf) as estados_ativas,
    
    -- Sector diversity
    count(distinct setor_economico) as setores_distintos,
    
    current_date as data_snapshot

from kpis_base

union all

-- Regional KPIs
select
    regiao as geografia,
    'REGIONAL' as segmento,
    sum(empresas_ativas) as total_empresas_ativas,
    round(sum(capital_total) / 1000000000, 2) as capital_total_bilhoes,
    round(avg(capital_medio), 2) as capital_medio_regional,
    
    sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) as empresas_novas_covid,
    round(
        sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) * 100.0 / sum(empresas_ativas), 2
    ) as percentual_empresas_novas,
    
    sum(empresas_tech) as total_empresas_tech,
    round(sum(empresas_tech) * 100.0 / sum(empresas_ativas), 2) as penetracao_tech_regional,
    
    sum(empresas_com_email) as empresas_digitais,
    round(sum(empresas_com_email) * 100.0 / sum(empresas_ativas), 2) as maturidade_digital,
    
    1 as regioes_ativas,
    count(distinct uf) as estados_regiao,
    
    count(distinct setor_economico) as setores_distintos,
    
    current_date as data_snapshot

from kpis_base
group by regiao

union all

-- Sector KPIs  
select
    setor_economico as geografia,
    'SETORIAL' as segmento,
    sum(empresas_ativas) as total_empresas_ativas,
    round(sum(capital_total) / 1000000000, 2) as capital_total_bilhoes,
    round(avg(capital_medio), 2) as capital_medio_setor,
    
    sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) as empresas_novas_covid,
    round(
        sum(case when ano_fundacao >= 2020 then empresas_ativas else 0 end) * 100.0 / sum(empresas_ativas), 2
    ) as percentual_empresas_novas,
    
    sum(empresas_tech) as total_empresas_tech,
    round(sum(empresas_tech) * 100.0 / sum(empresas_ativas), 2) as penetracao_tech_setor,
    
    sum(empresas_com_email) as empresas_digitais,
    round(sum(empresas_com_email) * 100.0 / sum(empresas_ativas), 2) as maturidade_digital,
    
    count(distinct regiao) as regioes_presentes,
    count(distinct uf) as estados_presentes,
    
    1 as setores_distintos,
    
    current_date as data_snapshot

from kpis_base
group by setor_economico

order by segmento, total_empresas_ativas desc