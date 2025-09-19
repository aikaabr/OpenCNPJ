{{
  config(
    materialized='view',
    docs={'description': 'Intermediate model for company ownership structure and partner analysis'}
  )
}}

select
    s.cnpj_basico,
    
    -- Partner counts by type
    count(*) as total_socios,
    count(case when s.tipo_socio = 'PESSOA_FISICA' then 1 end) as socios_pessoa_fisica,
    count(case when s.tipo_socio = 'PESSOA_JURIDICA' then 1 end) as socios_pessoa_juridica,
    count(case when s.tipo_socio = 'ESTRANGEIRO' then 1 end) as socios_estrangeiros,
    
    -- Partner diversity indicators
    count(distinct s.tipo_socio) as tipos_socios_distintos,
    count(distinct s.qualificacao_socio_codigo) as qualificacoes_distintas,
    
    -- Age group analysis (for individuals)
    count(case when s.faixa_etaria_desc = '21_30_ANOS' then 1 end) as socios_21_30,
    count(case when s.faixa_etaria_desc = '31_40_ANOS' then 1 end) as socios_31_40,
    count(case when s.faixa_etaria_desc = '41_50_ANOS' then 1 end) as socios_41_50,
    count(case when s.faixa_etaria_desc = '51_80_ANOS' then 1 end) as socios_51_80,
    
    -- Foreign participation
    case 
        when count(case when s.tipo_socio = 'ESTRANGEIRO' then 1 end) > 0 then true
        else false
    end as tem_participacao_estrangeira,
    
    -- Company network indicators (same partner in multiple companies)
    case 
        when count(case when s.tipo_socio = 'PESSOA_JURIDICA' then 1 end) > 0 then true
        else false
    end as tem_participacao_corporativa,
    
    -- Governance complexity
    case 
        when count(*) = 1 then 'SIMPLES'
        when count(*) between 2 and 5 then 'MEDIA'
        when count(*) > 5 then 'COMPLEXA'
        else 'NAO_INFORMADO'
    end as complexidade_societaria

from {{ ref('stg_socios') }} s

group by s.cnpj_basico