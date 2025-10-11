# Changelog - Correções do Pipeline DuckDB S3

## 2025-10-11 - Correções de Configuração e Ajustes nos Orchestrators

### Scripts Standalone (`scripts/`)

#### ✅ Corrigido: `data_lake_ingester.py`
- **Problema**: Upload para bucket errado (`dataeng-landing-zone-957` via `s3_bucket_name`)
- **Solução**: Alterado para usar `bronze_bucket` da seção `[datalake]` do config.ini
- **Arquivo**: `scripts/data_lake_ingester.py:71`

#### ✅ Corrigido: `data_lake_transformer.py`
- **Problema 1**: DuckDB não conseguia acessar S3 (erro 404) por falta de configuração de região
- **Solução 1**: Adicionado `SET s3_region='{aws_region}'` na configuração DuckDB
- **Arquivo**: `scripts/data_lake_transformer.py:46-54`

- **Problema 2**: Tipo de retorno incorreto do método `transform()`
- **Solução 2**: Alterado de `DuckDBPyRelation` para `None`
- **Arquivo**: `scripts/data_lake_transformer.py:61`

#### ✅ Corrigido: `main_agg.py`
- **Problema**: Logger usando `logger.info` em vez de `logger.success`
- **Solução**: Alterado para `logger.success` para consistência
- **Arquivo**: `scripts/main_agg.py:20`

### Dagster (`dagster_project/`)

#### ✅ Corrigido: `resources/duckdb_s3.py`
- **Problema 1**: Bucket hardcoded (`dataeng-landing-zone-957`)
- **Solução 1**: Alterado para usar variável de ambiente `bronze_bucket`
- **Arquivo**: `dagster_project/dagster_project/resources/duckdb_s3.py:70`

- **Problema 2**: Falta de configuração de região S3 no DuckDB
- **Solução 2**: Adicionado `SET s3_region='{aws_region}'`
- **Arquivo**: `dagster_project/dagster_project/resources/duckdb_s3.py:101-109`

- **Problema 3**: Tipo de retorno incorreto do método `transform()`
- **Solução 3**: Alterado de `DuckDBPyRelation` para `None`
- **Arquivo**: `dagster_project/dagster_project/resources/duckdb_s3.py:118`

#### ✅ Corrigido: `assets/duckdb_s3.py`
- **Problema**: Asset `aggregate_data` usando data com hora em vez de data diária
- **Solução**: Adicionado `agg_date = process_date.replace(hour=0, minute=0, second=0, microsecond=0)`
- **Arquivo**: `dagster_project/dagster_project/assets/duckdb_s3.py:79-85`

- **Melhoria**: Adicionado logging detalhado em todos os assets
- **Melhoria**: Adicionado `MaterializeResult` com metadata em todos os assets

#### ✅ Adicionado: Dependências no `pyproject.toml`
- Adicionadas dependências faltantes: `boto3`, `duckdb`, `requests`, `loguru`, `python-dotenv`
- **Arquivo**: `dagster_project/pyproject.toml`

#### ✅ Adicionado: Configuração via `.env`
- **Problema**: `EnvVar().get_value()` retornando `None` causando `TypeError` no S3 upload
- **Solução**: Criado sistema de configuração via arquivo `.env`
- **Arquivos**:
  - `dagster_project/.env` - Arquivo de configuração (gitignored)
  - `dagster_project/.env.example` - Template de exemplo
  - `dagster_project/dagster_project/definitions.py` - Carrega `.env` automaticamente via `python-dotenv`

### Airflow (`airflow_prj/`)

#### ✅ Corrigido: `include/resources/duckdb_s3.py`
- **Problema 1**: Bucket hardcoded (`dataeng-landing-zone-957`)
- **Solução 1**: Alterado para usar Airflow Variable `bronze_bucket`
- **Arquivo**: `airflow_prj/include/resources/duckdb_s3.py:68`

- **Problema 2**: Falta de configuração de região S3 no DuckDB
- **Solução 2**: Adicionado `SET s3_region='{aws_region}'`
- **Arquivo**: `airflow_prj/include/resources/duckdb_s3.py:99-107`

- **Problema 3**: Tipo de retorno incorreto do método `transform()`
- **Solução 3**: Alterado de `DuckDBPyRelation` para `None`
- **Arquivo**: `airflow_prj/include/resources/duckdb_s3.py:116`

#### ✅ Corrigido: `dags/duckdb_s3.py`
- **Problema**: Task `aggregate` usando data com hora em vez de data diária
- **Solução**: Adicionado `agg_date = partition_date.set(hour=0, minute=0, second=0, microsecond=0)`
- **Arquivo**: `airflow_prj/dags/duckdb_s3.py:35-38`

## Validação

### ✅ Pipeline End-to-End Testado

**Camadas validadas:**
- **Bronze**: 2 arquivos JSON.gz (205.1 KB) - Ingestão do GH Archive
- **Silver**: 2 arquivos Parquet (97.8 KB) - Transformação e limpeza
- **Gold**: 1 arquivo Parquet (58.8 KB) - Agregação diária

**Redução de tamanho**: ~71% (205KB → 59KB no final)

### ✅ Orchestrators Validados

**Dagster**:
- ✅ Assets carregando corretamente: `ingest_data`, `transform_data`, `aggregate_data`
- ✅ Job `duckdb_s3_job` definido com sucesso
- ✅ Schedule `duckdb_s3_schedule` configurado (cron: `0 * * * *`)
- ✅ Dependências instaladas via `uv add`

**Airflow**:
- ✅ DAG `duckdb_s3` estruturado corretamente
- ✅ Tasks sequenciais: `ingest_hourly_gharchive` → `transform` → `aggregate`
- ✅ Configuração via Airflow Variables

## Configuração Necessária

### Para Dagster
1. Copie o arquivo de exemplo: `cp dagster_project/.env.example dagster_project/.env`
2. Edite o arquivo `.env` com suas credenciais:
```bash
s3_access_key_id=<sua_access_key>
s3_secret_access_key=<sua_secret_key>
s3_region_name=us-east-1
bronze_bucket=bronze-bucket-duckdb
silver_bucket=silver-bucket-duckdb
gold_bucket=gold-bucket-duckdb
```

**Nota**: O arquivo `.env` é carregado automaticamente pelo `definitions.py` usando `python-dotenv`.

### Para Airflow
Configurar Airflow Variables via UI (Admin → Variables):
- `s3_access_key_id`
- `s3_secret_access_key`
- `s3_region_name`
- `bronze_bucket`
- `silver_bucket`
- `gold_bucket`

## Como Executar

### Scripts Standalone
```bash
# Ingerir dados (Bronze)
uv run scripts/main_ingest.py

# Transformar dados (Silver)
uv run scripts/main_transform.py

# Agregar dados (Gold)
uv run scripts/main_agg.py
```

### Dagster
```bash
cd dagster_project
dagster dev  # Inicia o servidor web em localhost:3000
```

### Airflow
```bash
cd airflow_prj
astro dev start  # Inicia o Airflow em localhost:8080
```

## Linting
✅ Todos os arquivos passaram no `ruff check .`
