# OpenCNPJ Analytics with dbt

Este diretório contém os modelos dbt para análise avançada dos dados do OpenCNPJ.

## Setup Rápido

```bash
# Via Docker Compose (recomendado)
docker-compose up opencnpj-analytics

# Executar modelos
docker-compose exec opencnpj-analytics dbt run

# Executar apenas modelos G2Market
docker-compose exec opencnpj-analytics dbt run --select marts.g2market

# Gerar documentação
docker-compose exec opencnpj-analytics dbt docs generate
docker-compose exec opencnpj-analytics dbt docs serve --host 0.0.0.0
```

## Estrutura dos Modelos

### Staging (`models/staging/`)
Dados limpos e padronizados a partir das fontes brutas:
- `stg_empresas`: Dados básicos das empresas com limpeza e tipagem
- `stg_estabelecimentos`: Estabelecimentos com geocoding e formatação
- `stg_socios`: Estrutura societária normalizada
- `stg_regime_tributario`: Informações de Simples Nacional e MEI
- `stg_cnae`: Códigos de atividade econômica com classificação setorial
- `stg_municipios`: Municípios com informações geográficas

### Intermediate (`models/intermediate/`)
Transformações de negócio e agregações:
- `int_empresas_completo`: Visão consolidada de empresa + estabelecimento
- `int_estrutura_societaria`: Análise da complexidade societária

### Marts Core (`models/marts/core/`)
Dimensões e fatos para análises gerais:
- `dim_empresas`: Dimensão principal de empresas

### Marts G2Market (`models/marts/g2market/`)
Modelos específicos para estratégia G2Market:
- `segmentacao_empresas`: Score e segmentação para vendas B2B
- `distribuicao_geografica_setorial`: Distribuição de mercado
- `oportunidades_mercado`: Análise de oportunidades com estratégias
- `kpis_dashboard`: Indicadores executivos

## Exemplos de Queries

### 1. Top 100 Prospects G2Market
```sql
SELECT 
    cnpj_completo,
    razao_social_clean,
    score_g2market,
    segmento_g2market,
    acao_recomendada,
    setor_economico,
    uf,
    capital_social_valor,
    email_clean
FROM {{ ref('segmentacao_empresas') }}
WHERE segmento_g2market IN ('ALTO_POTENCIAL', 'MEDIO_POTENCIAL')
  AND email_clean IS NOT NULL
ORDER BY score_g2market DESC
LIMIT 100;
```

### 2. Melhores Mercados para Expansão
```sql
SELECT 
    regiao,
    uf,
    setor_economico,
    empresas_qualificadas,
    penetracao_tech,
    capital_medio,
    classificacao_oportunidade,
    estrategia_entrada
FROM {{ ref('oportunidades_mercado') }}
WHERE classificacao_oportunidade IN ('OPORTUNIDADE_PREMIUM', 'OPORTUNIDADE_ALTA')
  AND empresas_qualificadas >= 50
ORDER BY score_oportunidade DESC;
```

### 3. KPIs Nacional vs Regional
```sql
SELECT 
    geografia,
    segmento,
    total_empresas_ativas,
    capital_total_bilhoes,
    penetracao_tech_nacional,
    maturidade_digital
FROM {{ ref('kpis_dashboard') }}
WHERE segmento IN ('GERAL', 'REGIONAL')
ORDER BY total_empresas_ativas DESC;
```

### 4. Empresas Tech por Estado
```sql
SELECT 
    uf,
    COUNT(*) as total_empresas,
    SUM(CASE WHEN is_tech_sector THEN 1 ELSE 0 END) as empresas_tech,
    ROUND(
        SUM(CASE WHEN is_tech_sector THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) as percentual_tech,
    AVG(capital_social_valor) as capital_medio
FROM {{ ref('dim_empresas') }}
WHERE is_empresa_ativa = true
GROUP BY uf
HAVING total_empresas >= 1000
ORDER BY percentual_tech DESC;
```

## Configuração de Performance

Para datasets grandes, ajuste as configurações no `dbt_project.yml`:

```yaml
vars:
  # Filtros para otimização
  min_company_capital: 10000
  start_date: '2020-01-01'
  batch_size: 100000

models:
  opencnpj_analytics:
    marts:
      g2market:
        +materialized: table
        +indexes:
          - cnpj_basico
          - score_g2market
```

## Desenvolvimento

### Adicionando Novos Modelos

1. Crie o arquivo SQL em `models/marts/g2market/`
2. Adicione configuração no `_models.yml`
3. Teste localmente: `dbt run --select nome_do_modelo`
4. Adicione testes de data quality
5. Documente o modelo

### Testes de Qualidade

```bash
# Executar todos os testes
dbt test

# Testar modelo específico
dbt test --select segmentacao_empresas

# Teste apenas freshness
dbt source freshness
```

### Debugging

```bash
# Verificar configuração
dbt debug

# Compilar modelo sem executar
dbt compile --select nome_do_modelo

# Executar em modo debug
dbt run --select nome_do_modelo --debug
```

## Integração com BI Tools

Os modelos podem ser conectados diretamente a ferramentas como:
- **Metabase**: Conecte ao DuckDB em `/app/data/analytics.duckdb`
- **Grafana**: Use o plugin DuckDB
- **Tableau**: Via ODBC DuckDB driver
- **Power BI**: Export para CSV ou conecte via ODBC

## Automação

Para executar os modelos automaticamente:

```bash
# Via cron (exemplo diário às 6h)
0 6 * * * cd /path/to/Analytics && dbt run --target prod

# Via Docker Compose (uncomment no docker-compose.yml)
command: ["dbt", "run", "--target", "prod"]
```