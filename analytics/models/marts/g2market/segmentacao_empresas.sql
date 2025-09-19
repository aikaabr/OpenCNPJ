{{
  config(
    materialized='table',
    docs={'description': 'Company segmentation analysis for targeted G2Market campaigns'}
  )
}}

with empresa_scores as (
    select
        ec.*,
        es.total_socios,
        es.complexidade_societaria,
        es.tem_participacao_estrangeira,
        
        -- Scoring for G2Market potential
        (case when ec.is_empresa_ativa then 20 else 0 end +
         case when ec.porte_empresa_desc = 'PEQUENA_EMPRESA' then 15
              when ec.porte_empresa_desc = 'DEMAIS_EMPRESAS' then 25
              else 5 end +
         case when ec.is_tech_sector then 25 else 0 end +
         case when ec.anos_funcionamento >= 2 then 15 else 0 end +
         case when ec.is_capital_significativo then 20 else 0 end +
         case when ec.email_clean is not null then 10 else 0 end +
         case when es.complexidade_societaria = 'MEDIA' then 10
              when es.complexidade_sociedaria = 'COMPLEXA' then 15
              else 0 end
        ) as score_g2market
        
    from {{ ref('int_empresas_completo') }} ec
    left join {{ ref('int_estrutura_societaria') }} es
        on ec.cnpj_basico = es.cnpj_basico
    
    where ec.is_empresa_ativa = true
)

select
    -- Company identification
    cnpj_completo,
    cnpj_basico,
    razao_social_clean,
    nome_fantasia_clean,
    
    -- Segmentation
    case 
        when score_g2market >= 80 then 'ALTO_POTENCIAL'
        when score_g2market >= 60 then 'MEDIO_POTENCIAL'
        when score_g2market >= 40 then 'BAIXO_POTENCIAL'
        else 'NAO_QUALIFICADO'
    end as segmento_g2market,
    
    score_g2market,
    
    -- Business profile
    setor_economico,
    atividade_principal,
    porte_empresa_desc,
    capital_social_valor,
    anos_funcionamento,
    regime_tributario_atual,
    
    -- Geographic targeting
    regiao,
    uf,
    municipio_nome,
    is_capital,
    
    -- Technology indicators
    is_tech_sector,
    
    -- Contact information
    email_clean,
    telefone1_formatado,
    
    -- Partnership characteristics
    total_socios,
    complexidade_societaria,
    tem_participacao_estrangeira,
    
    -- Recommended action
    case 
        when score_g2market >= 80 and is_tech_sector then 'PRIORIDADE_MAXIMA'
        when score_g2market >= 80 then 'CONTATO_DIRETO'
        when score_g2market >= 60 and email_clean is not null then 'EMAIL_MARKETING'
        when score_g2market >= 40 then 'NURTURING_CAMPAIGN'
        else 'MONITORAMENTO'
    end as acao_recomendada,
    
    current_date as analysis_date

from empresa_scores

where score_g2market >= 30  -- Minimum threshold for inclusion

order by score_g2market desc, capital_social_valor desc