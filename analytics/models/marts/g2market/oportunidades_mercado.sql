{{
  config(
    materialized='table',
    docs={'description': 'Market opportunities analysis for G2Market business development'}
  )
}}

with oportunidades_base as (
    select
        regiao,
        uf,
        setor_economico,
        
        -- Market size indicators
        count(*) as total_empresas_ativas,
        count(case when porte_empresa_desc in ('PEQUENA_EMPRESA', 'DEMAIS_EMPRESAS') then 1 end) as empresas_qualificadas,
        
        -- Capital potential
        sum(capital_social_valor) as capital_total_setor,
        avg(capital_social_valor) as capital_medio_setor,
        
        -- Technology penetration
        count(case when is_tech_sector then 1 end) as empresas_tech,
        round(count(case when is_tech_sector then 1 end) * 100.0 / count(*), 2) as penetracao_tech,
        
        -- Contact readiness
        count(case when email_clean is not null then 1 end) as empresas_com_contato,
        round(count(case when email_clean is not null then 1 end) * 100.0 / count(*), 2) as percentual_contatavel,
        
        -- Business maturity
        avg(anos_funcionamento) as maturidade_media,
        count(case when anos_funcionamento between 2 and 10 then 1 end) as empresas_crescimento,
        
        -- Competition density
        count(*) * 1.0 / count(distinct municipio_nome) as densidade_competitiva
        
    from {{ ref('int_empresas_completo') }}
    where is_empresa_ativa = true
    group by regiao, uf, setor_economico
),

ranking_oportunidades as (
    select 
        *,
        -- Opportunity scoring
        (case when empresas_qualificadas >= 100 then 25
              when empresas_qualificadas >= 50 then 20
              when empresas_qualificadas >= 20 then 15
              else 10 end +
         case when capital_medio_setor >= 100000 then 20
              when capital_medio_setor >= 50000 then 15
              when capital_medio_setor >= 10000 then 10
              else 5 end +
         case when penetracao_tech >= 20 then 25
              when penetracao_tech >= 10 then 20
              when penetracao_tech >= 5 then 15
              else 10 end +
         case when percentual_contatavel >= 60 then 15
              when percentual_contatavel >= 40 then 10
              else 5 end +
         case when maturidade_media between 3 and 8 then 15 else 10 end
        ) as score_oportunidade,
        
        row_number() over (order by 
            empresas_qualificadas desc, 
            capital_medio_setor desc, 
            penetracao_tech desc
        ) as ranking_geral
        
    from oportunidades_base
    where empresas_qualificadas >= 10  -- Minimum market size
)

select
    regiao,
    uf,
    setor_economico,
    
    -- Market metrics
    total_empresas_ativas,
    empresas_qualificadas,
    round(capital_total_setor / 1000000, 2) as capital_total_milhoes,
    round(capital_medio_setor, 2) as capital_medio,
    
    -- Technology and innovation
    empresas_tech,
    penetracao_tech,
    
    -- Accessibility
    empresas_com_contato,
    percentual_contatavel,
    
    -- Market characteristics
    round(maturidade_media, 1) as maturidade_media_anos,
    empresas_crescimento,
    round(densidade_competitiva, 2) as densidade_competitiva,
    
    -- Opportunity assessment
    score_oportunidade,
    ranking_geral,
    
    case 
        when score_oportunidade >= 80 then 'OPORTUNIDADE_PREMIUM'
        when score_oportunidade >= 65 then 'OPORTUNIDADE_ALTA'
        when score_oportunidade >= 50 then 'OPORTUNIDADE_MEDIA'
        else 'OPORTUNIDADE_BAIXA'
    end as classificacao_oportunidade,
    
    -- Strategic recommendations
    case 
        when score_oportunidade >= 80 and penetracao_tech >= 15 
        then 'ENTRADA_IMEDIATA'
        when score_oportunidade >= 65 and empresas_qualificadas >= 50 
        then 'PLANEJAMENTO_RAPIDO'
        when score_oportunidade >= 50 
        then 'ANALISE_DETALHADA'
        else 'MONITORAMENTO'
    end as estrategia_entrada,
    
    current_date as analysis_date

from ranking_oportunidades

order by score_oportunidade desc, empresas_qualificadas desc