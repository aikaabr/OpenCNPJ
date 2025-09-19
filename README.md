<img src="./Page/assets/logo.svg" alt="OpenCNPJ" height="64" />

# OpenCNPJ

Projeto aberto para baixar, processar e publicar dados públicos das empresas do Brasil com capacidades analíticas avançadas.

## 🚀 Funcionalidades

- **ETL Completo**: Download, processamento e publicação automatizada dos dados de CNPJ da Receita Federal
- **Múltiplos Storages**: Suporte flexível para filesystem local, rclone ou S3 (rclone opcional)
- **Containerização**: Deploy simplificado com Docker e Docker Compose
- **Análise Avançada**: Camada analítica com dbt para insights de mercado e estratégia G2Market
- **Interface Web**: SPA para consulta e visualização dos dados

## 📁 Estrutura do Projeto

- `ETL/`: Pipeline ETL que baixa, processa e publica dados do CNPJ
- `Page/`: Página/SPA estática para consulta dos dados publicados
- `Analytics/`: Modelos dbt para análise de dados e business intelligence
- `docker/`: Configurações Docker e arquivos de deployment

## 🛠 Requisitos

### Execução Local
- `.NET SDK 9.0+`
- `Python 3.8+` (opcional, para dbt analytics)
- `rclone` (opcional, para storage remoto)
- Espaço em disco e boa conexão (primeira execução pode levar dias)

### Execução via Docker
- Docker e Docker Compose
- 4GB+ RAM recomendado
- Espaço em disco adequado para os dados

## ⚙️ Configuração

O OpenCNPJ agora oferece configuração flexível e centralizada através de variáveis de ambiente. Para configuração detalhada, consulte o [CONFIGURATION.md](./CONFIGURATION.md).

### 🚀 Configuração Rápida

1. **Copie o arquivo de exemplo:**
   ```bash
   cp .env.example .env
   ```

2. **Configure o diretório base de dados:**
   ```bash
   # Edite o arquivo .env e defina o diretório base
   OPENCNPJ_BASE_PATH=/caminho/para/seus/dados
   ```

3. **Valide a configuração:**
   ```bash
   cd ETL && dotnet run -- config
   ```

### 📁 Estrutura de Dados Configurável

O sistema organiza automaticamente os dados em uma estrutura hierárquica:

```
OPENCNPJ_BASE_PATH/
├── downloads/          # Downloads temporários da Receita Federal
├── extracted_data/     # Dados extraídos dos ZIPs
├── parquet_data/       # Arquivos Parquet processados
├── output/            # Arquivos NDJSON finais
├── hash_cache/        # Cache para evitar reprocessamento
├── temp/              # Arquivos temporários
├── logs/              # Logs do sistema
├── backups/           # Backups automáticos
└── config/            # Configurações específicas do ambiente
```

### Storage Options

O projeto agora suporta múltiplas opções de storage:

#### 1. FileSystem Local (Padrão)
```json
{
  "Storage": {
    "Type": "filesystem",
    "Enabled": true,
    "FileSystem": {
      "OutputPath": "./output"
    }
  }
}
```

#### 2. Rclone (Compatibilidade com versão anterior)
```json
{
  "Storage": {
    "Type": "rclone",
    "Enabled": true
  },
  "Rclone": {
    "RemoteBase": "Opencnpj:opencnpj/files",
    "Transfers": 100,
    "MaxConcurrentUploads": 4
  }
}
```

#### 3. S3 (Em desenvolvimento)
```json
{
  "Storage": {
    "Type": "s3",
    "Enabled": true,
    "S3": {
      "BucketName": "my-bucket",
      "Region": "us-east-1",
      "AccessKey": "...",
      "SecretKey": "...",
      "Prefix": "opencnpj/"
    }
  }
}
```

### Configuração via Variáveis de Ambiente

O projeto suporta configuração flexível através de arquivo `.env`:

```bash
# Configuração básica para desenvolvimento
OPENCNPJ_BASE_PATH=./data
STORAGE_TYPE=filesystem
DUCKDB_IN_MEMORY=true

# Configuração para produção
OPENCNPJ_BASE_PATH=/opt/opencnpj/data
STORAGE_TYPE=s3
S3_BUCKET_NAME=meu-bucket-opencnpj
DUCKDB_IN_MEMORY=false
DUCKDB_MEMORY_LIMIT=16GB
```

**Comando de diagnóstico:**
```bash
cd ETL && dotnet run -- config
```

### Configuração Docker

1. **Copie o arquivo de exemplo:**
```bash
cp .env.example .env
```

2. **Configure as variáveis de ambiente no `.env`:**
```bash
# Storage Configuration
STORAGE_TYPE=filesystem  # ou 'rclone', 's3'
STORAGE_ENABLED=true

# Para rclone (se necessário)
RCLONE_REMOTE=myremote:path

# Para S3 (se necessário)
S3_BUCKET_NAME=my-bucket
S3_ACCESS_KEY=...
S3_SECRET_KEY=...

# Performance
DUCKDB_MEMORY_LIMIT=8GB
NDJSON_MAX_PARALLEL=16
```

## 🐳 Execução com Docker

### Quick Start
```bash
# Build e start todos os serviços
docker-compose up -d

# Execute o pipeline ETL
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll pipeline

# Execute análises dbt
docker-compose exec opencnpj-analytics dbt run

# Acesse a interface web
open http://localhost:8080
```

### Comandos Úteis

```bash
# Apenas ETL
docker-compose up opencnpj-etl

# Pipeline específico com mês
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll pipeline -m 2024-01

# Build analytics
docker-compose exec opencnpj-analytics dbt build

# Teste de integridade
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll test

# Logs
docker-compose logs -f opencnpj-etl
```

## � Quick Start

### Primeiro setup
```bash
# Instalar dependências
./opencnpj.sh setup

# Configurar para uso local (sem upload remoto)
./opencnpj.sh storage local

# Executar pipeline completo
./opencnpj.sh pipeline
```

### Comandos principais
```bash
# Pipeline ETL
./opencnpj.sh pipeline

# Analytics com dbt
./opencnpj.sh analytics

# Interface web
./opencnpj.sh web

# Tudo junto
./opencnpj.sh all

# Ver todas as opções
./opencnpj.sh help
```

### Configuração de Storage
```bash
# Local apenas (recomendado para teste)
./opencnpj.sh storage local

# Desabilitar qualquer storage
./opencnpj.sh storage disable

# Rclone (requer configuração)
./opencnpj.sh storage rclone

# S3 (requer configuração)
./opencnpj.sh storage s3
```

## 🐳 Execução com Docker

### Quick Start
```bash
# Build e start todos os serviços
docker-compose up -d

# Execute o pipeline ETL
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll pipeline

# Execute análises dbt
docker-compose exec opencnpj-analytics dbt run

# Acesse a interface web
open http://localhost:8080
```

### Comandos Docker Úteis
```bash
# Apenas ETL
docker-compose up opencnpj-etl

# Pipeline específico com mês
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll pipeline -m 2024-01

# Build analytics
docker-compose exec opencnpj-analytics dbt build

# Teste de integridade
docker-compose exec opencnpj-etl dotnet CNPJExporter.dll test

# Logs
docker-compose logs -f opencnpj-etl
```

## 📊 Análise de Dados com dbt

### Estrutura Analítica

```
Analytics/
├── models/
│   ├── staging/          # Dados limpos e padronizados
│   ├── intermediate/     # Transformações de negócio
│   └── marts/
│       ├── core/         # Dimensões e fatos gerais
│       └── g2market/     # Análises específicas G2Market
```

### Modelos G2Market Disponíveis

1. **`segmentacao_empresas`**: Segmentação de empresas com scoring para estratégia G2Market
2. **`distribuicao_geografica_setorial`**: Distribuição geográfica e setorial para planejamento de mercado
3. **`oportunidades_mercado`**: Análise de oportunidades com recomendações de entrada
4. **`kpis_dashboard`**: KPIs executivos para dashboard

### Executando Análises

```bash
# Via Docker
docker-compose exec opencnpj-analytics dbt run
docker-compose exec opencnpj-analytics dbt test
docker-compose exec opencnpj-analytics dbt docs generate

# Local (dentro da pasta Analytics)
dbt run
dbt test
dbt docs generate && dbt docs serve
```

### Exemplos de Análises G2Market

```sql
-- Empresas de alto potencial para G2Market
SELECT 
    razao_social_clean,
    segmento_g2market,
    score_g2market,
    acao_recomendada,
    setor_economico,
    uf
FROM segmentacao_empresas 
WHERE segmento_g2market = 'ALTO_POTENCIAL'
ORDER BY score_g2market DESC;

-- Oportunidades de mercado por região
SELECT 
    regiao,
    setor_economico,
    empresas_qualificadas,
    classificacao_oportunidade,
    estrategia_entrada
FROM oportunidades_mercado 
WHERE classificacao_oportunidade IN ('OPORTUNIDADE_PREMIUM', 'OPORTUNIDADE_ALTA')
ORDER BY score_oportunidade DESC;
```

## 🎯 Use Cases G2Market

### 1. Identificação de Prospects
- **Modelo**: `segmentacao_empresas`
- **Objetivo**: Identificar empresas com alto potencial para soluções B2B
- **Filtros**: Score G2Market, setor tecnológico, porte empresa

### 2. Planejamento de Expansão Geográfica
- **Modelo**: `distribuicao_geografica_setorial`
- **Objetivo**: Identificar regiões com maior concentração do público-alvo
- **Métricas**: Densidade empresarial, penetração tech, maturidade digital

### 3. Análise de Mercado
- **Modelo**: `oportunidades_mercado`
- **Objetivo**: Priorizar mercados para entrada
- **Indicadores**: Score de oportunidade, estratégia de entrada, competição

### 4. Dashboard Executivo
- **Modelo**: `kpis_dashboard`
- **Objetivo**: Monitorar indicadores-chave do mercado brasileiro
- **Métricas**: Total empresas ativas, penetração tech, capital médio

## 📈 Volumes de Dados

- **Empresas**: ~50M registros
- **Estabelecimentos**: ~55M registros
- **Sócios**: ~20M registros
- **Volume total descompactado**: ~550GB
- **Volume processado (analytics)**: Varia conforme modelos

## 🔧 Troubleshooting

### Problemas Comuns

1. **Memória insuficiente**: Ajuste `DUCKDB_MEMORY_LIMIT` no `.env`
2. **Storage indisponível**: Verifique `STORAGE_TYPE` e credenciais
3. **dbt connection error**: Confirme que os dados estão processados no DuckDB

### Logs e Debugging

```bash
# Logs ETL
docker-compose logs opencnpj-etl

# Logs analytics
docker-compose logs opencnpj-analytics

# Debug dbt
docker-compose exec opencnpj-analytics dbt debug
```

## 🤝 Contribuição

- Abra issues para discutir mudanças
- Faça fork, crie uma branch descritiva e envie PR
- Mantenha commits pequenos e o projeto compilando
- Para mudanças nos modelos dbt, inclua testes

## 📋 Backlog

- [ ] Implementação completa do S3 Exporter
- [ ] Dashboard web para visualização dos modelos dbt
- [ ] API REST para acesso aos dados analíticos
- [ ] Modelos de ML para previsão de crescimento empresarial
- [ ] Integração com ferramentas de CRM/Sales

## 📄 Licença

Este projeto está sob licença MIT. Veja o arquivo LICENSE para detalhes.
