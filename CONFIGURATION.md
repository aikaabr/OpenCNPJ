# Configuração do OpenCNPJ

O OpenCNPJ suporta configuração flexível através de variáveis de ambiente e arquivos de configuração. Este documento descreve como configurar o sistema para diferentes cenários.

## 🎯 Configuração Rápida

1. **Copie o arquivo de exemplo:**
   ```bash
   cp .env.example .env
   ```

2. **Configure o diretório base de dados:**
   ```bash
   # Edite o arquivo .env
   OPENCNPJ_BASE_PATH=/caminho/para/seus/dados
   ```

3. **Valide a configuração:**
   ```bash
   ./ETL/bin/Release/net9.0/CNPJExporter config
   ```

## 📁 Estrutura de Paths

O sistema organiza os dados em uma estrutura hierárquica baseada em um diretório base:

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

## 🔧 Variáveis de Ambiente

### Paths Principais

| Variável | Descrição | Padrão |
|----------|-----------|---------|
| `OPENCNPJ_BASE_PATH` | Diretório raiz para todos os dados | `./opencnpj_data` |
| `OPENCNPJ_DOWNLOAD_PATH` | Downloads temporários | `{BASE_PATH}/downloads` |
| `OPENCNPJ_EXTRACTED_DATA_PATH` | Dados extraídos | `{BASE_PATH}/extracted_data` |
| `OPENCNPJ_PARQUET_DATA_PATH` | Arquivos Parquet | `{BASE_PATH}/parquet_data` |
| `OPENCNPJ_OUTPUT_PATH` | Arquivos de saída | `{BASE_PATH}/output` |
| `OPENCNPJ_HASH_CACHE_PATH` | Cache de hashes | `{BASE_PATH}/hash_cache` |
| `OPENCNPJ_TEMP_PATH` | Arquivos temporários | `{BASE_PATH}/temp` |
| `OPENCNPJ_LOGS_PATH` | Logs do sistema | `{BASE_PATH}/logs` |
| `OPENCNPJ_BACKUP_PATH` | Backups | `{BASE_PATH}/backups` |
| `OPENCNPJ_CONFIG_PATH` | Configurações | `{BASE_PATH}/config` |

### Armazenamento

| Variável | Descrição | Padrão |
|----------|-----------|---------|
| `STORAGE_TYPE` | Tipo de armazenamento (`filesystem`, `rclone`, `s3`) | `filesystem` |
| `STORAGE_ENABLED` | Habilitar upload para armazenamento externo | `true` |
| `FILESYSTEM_OUTPUT_PATH` | Path para armazenamento local | `{OUTPUT_PATH}` |

### DuckDB

| Variável | Descrição | Padrão |
|----------|-----------|---------|
| `DUCKDB_IN_MEMORY` | Usar DuckDB em memória | `true` |
| `DUCKDB_THREADS` | Número de threads (0 = automático) | `0` |
| `DUCKDB_MEMORY_LIMIT` | Limite de memória (ex: 5GB) | `5GB` |
| `DUCKDB_ENGINE_THREADS` | Threads do engine (0 = automático) | `0` |
| `DUCKDB_PRESERVE_ORDER` | Preservar ordem de inserção | `false` |

### Processamento

| Variável | Descrição | Padrão |
|----------|-----------|---------|
| `NDJSON_BATCH_SIZE` | Tamanho do lote para upload | `10000` |
| `NDJSON_NORMALIZE` | Normalizar antes de gerar hash | `false` |
| `NDJSON_WRITE_FILES` | Escrever arquivos JSON intermediários | `false` |
| `NDJSON_MAX_PARALLEL` | Máximo processamento paralelo | `8` |
| `DOWNLOADER_PARALLEL` | Downloads paralelos | `6` |

## 🎛️ Cenários de Configuração

### Desenvolvimento Local

```bash
# .env para desenvolvimento
OPENCNPJ_BASE_PATH=./data
STORAGE_TYPE=filesystem
DUCKDB_IN_MEMORY=true
DOWNLOADER_PARALLEL=2
```

### Produção com Armazenamento Local

```bash
# .env para produção local
OPENCNPJ_BASE_PATH=/opt/opencnpj/data
STORAGE_TYPE=filesystem
DUCKDB_IN_MEMORY=false
DUCKDB_MEMORY_LIMIT=16GB
DOWNLOADER_PARALLEL=8
```

### Produção com S3

```bash
# .env para produção com S3
OPENCNPJ_BASE_PATH=/opt/opencnpj/data
STORAGE_TYPE=s3
S3_BUCKET_NAME=meu-bucket-opencnpj
S3_REGION=us-east-1
S3_ACCESS_KEY=sua_access_key
S3_SECRET_KEY=sua_secret_key
DUCKDB_IN_MEMORY=false
DUCKDB_MEMORY_LIMIT=32GB
```

### Produção com Google Drive (rclone)

```bash
# .env para produção com Google Drive
OPENCNPJ_BASE_PATH=/opt/opencnpj/data
STORAGE_TYPE=rclone
RCLONE_REMOTE=gdrive:opencnpj/dados
RCLONE_TRANSFERS=50
RCLONE_MAX_CONCURRENT=8
```

## 📊 Monitoramento de Configuração

Use o comando `config` para verificar sua configuração atual:

```bash
./ETL/bin/Release/net9.0/CNPJExporter config
```

Este comando exibe:
- ✅ Validação de acesso aos diretórios
- 📁 Todos os paths configurados
- ⚙️ Configurações atuais do sistema
- 💡 Dicas de otimização

## 🔄 Hierarquia de Configuração

O sistema aplica configurações na seguinte ordem (última prevalece):

1. **Valores padrão** (hardcoded no código)
2. **Arquivo config.json** (se existir)
3. **Variáveis de ambiente do sistema**
4. **Arquivo .env** (se existir)

## 🛠️ Troubleshooting

### Problema: Diretórios não são criados

**Solução:** Verifique permissões do diretório base:
```bash
# Verificar permissões
ls -la $(dirname $OPENCNPJ_BASE_PATH)

# Corrigir permissões se necessário
chmod 755 $OPENCNPJ_BASE_PATH
```

### Problema: Configuração não está sendo aplicada

**Solução:** Verifique se o arquivo .env está no diretório correto:
```bash
# O arquivo .env deve estar no mesmo diretório do executável
ls -la .env

# Ou usar path absoluto
OPENCNPJ_BASE_PATH=/caminho/absoluto/para/dados
```

### Problema: Performance baixa

**Solução:** Ajuste configurações de processamento:
```bash
# Para máquinas com mais recursos
DUCKDB_MEMORY_LIMIT=16GB
DUCKDB_THREADS=8
DOWNLOADER_PARALLEL=12
NDJSON_MAX_PARALLEL=16
```

## 🔐 Segurança

- **Nunca** commite o arquivo `.env` com credenciais
- Use variáveis de ambiente do sistema para credenciais sensíveis em produção
- Configure permissões apropriadas nos diretórios de dados
- Use criptografia para backups sensíveis

## 📖 Referências

- [Documentação DuckDB](https://duckdb.org/docs/)
- [Configuração Rclone](https://rclone.org/docs/)
- [AWS S3 Configuration](https://docs.aws.amazon.com/s3/)