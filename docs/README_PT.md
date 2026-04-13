# Netflix Data Pipeline — Documentação Completa (PT)

> **Voltar ao overview:** [README.md](../README.md) | [English Version](README_EN.md)

---

## Sumário

1. [Visão Geral do Projeto](#1-visão-geral-do-projeto)
2. [Arquitetura](#2-arquitetura)
3. [Stack Tecnológica](#3-stack-tecnológica)
4. [Fontes de Dados](#4-fontes-de-dados)
5. [Camadas do Pipeline](#5-camadas-do-pipeline)
   - 5.1 [Bronze — Ingestão Bruta (GCS)](#51-bronze--ingestão-bruta-gcs)
   - 5.2 [Silver — Modelo Dimensional (BigQuery)](#52-silver--modelo-dimensional-bigquery)
   - 5.3 [Gold — Views Analíticas (BigQuery)](#53-gold--views-analíticas-bigquery)
6. [Modelo de Dados](#6-modelo-de-dados)
7. [Qualidade de Dados](#7-qualidade-de-dados)
8. [Dashboard — Metabase](#8-dashboard--metabase)
9. [Avaliação do Modelo & Proposta de Negócio](#9-avaliação-do-modelo--proposta-de-negócio)
10. [Como Executar](#10-como-executar)
11. [Estrutura do Projeto](#11-estrutura-do-projeto)
12. [Referências](#12-referências)

---

## 1. Visão Geral do Projeto

Este projeto implementa um **pipeline de engenharia de dados completo** utilizando datasets reais de recomendação da Netflix. Foi construído a partir de uma videoaula do YouTube como ponto de partida e expandido com camadas analíticas adicionais, modelagem dimensional customizada, verificações de qualidade de dados e uma análise orientada a negócio sobre a avaliação do modelo de recomendação.

O pipeline percorre desde arquivos CSV brutos armazenados no Google Cloud Storage até dashboards interativos no Metabase, passando por um modelo dimensional bem estruturado no BigQuery. Adicionalmente, métricas de qualidade do modelo de recomendação (MAE, RMSE, Bias) foram calculadas e, diante dos resultados insatisfatórios, uma apresentação de negócio foi produzida propondo uma nova arquitetura de modelo.

---

## 2. Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                     FONTES DE DADOS (CSV)                       │
│  movies | user_rating_history | additional_ratings |            │
│  belief_data | movie_elicitation_set | recommendation_history   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│             BRONZE — Google Cloud Storage (GCS)                 │
│        gs://netflix-data-video-amura/bronze/*.csv               │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Tabelas Externas
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│             SILVER — BigQuery (netflix_raw)                     │
│   raw_movies | raw_user_rating_history |                        │
│   raw_user_additional_rating | raw_belief_data |                │
│   raw_movie_elicitation_set | raw_user_recommendation_history   │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Modelagem Dimensional
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│          SILVER — BigQuery (netflix_analytical)                 │
│   dim_movie | dim_user | dim_date                               │
│   fact_user_rating | fact_belief | fact_recommendation          │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Views Analíticas
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│           GOLD — Views BigQuery (netflix_analytical)            │
│   vw_user_kpis | vw_movie_kpis | vw_top_10_movies              │
│   vw_genre_performance | vw_ratings_temporal |                  │
│   vw_movies_50_plus | vw_recommendation_quality_metrics |       │
│   vw_recommendation_error_by_genre | vw_recommendation_error_by_movie │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   METABASE (Docker)                             │
│        Dashboards de Negócio + Avaliação do Modelo              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Stack Tecnológica

| Camada | Tecnologia |
|---|---|
| Armazenamento Bruto | Google Cloud Storage (GCS) |
| Data Warehouse | Google BigQuery |
| Containerização | Docker |
| Business Intelligence | Metabase |
| Dialeto SQL | BigQuery Standard SQL |

---

## 4. Fontes de Dados

Todos os arquivos são CSVs no bucket GCS `gs://netflix-data-video-amura/bronze/`.

| Arquivo | Descrição |
|---|---|
| `movies.csv` | Catálogo de filmes: movieId, title, genres (separados por pipe) |
| `user_rating_history.csv` | Avaliações históricas dos usuários: userId, movieId, rating, tstamp |
| `ratings_for_additional_users.csv` | Avaliações suplementares de uma nova coorte de usuários |
| `belief_data.csv` | Estudo de elicitação de crenças: predições do usuário e do sistema, certeza, status de visualização |
| `movie_elicitation_set.csv` | Filmes selecionados para o estudo de elicitação |
| `user_recommendation_history.csv` | Recomendações geradas pelo sistema com ratings previstos |

---

## 5. Camadas do Pipeline

### 5.1 Bronze — Ingestão Bruta (GCS)

Os arquivos CSV brutos são depositados no bucket GCS. Nenhuma transformação ocorre nesta etapa — é a fonte única da verdade para todo o processamento downstream.

### 5.2 Silver — Modelo Dimensional (BigQuery)

**Tabelas Externas (schema netflix_raw)**

Tabelas externas do BigQuery são criadas apontando diretamente para os CSVs no GCS. Isso evita duplicação de dados e permite consultas SQL sobre os arquivos brutos.

**Tabelas Dimensionais (schema netflix_analytical)**

Três tabelas de dimensão e três tabelas fato são construídas:

**`dim_movie`** — Dimensão de filmes com chave surrogate (`movie_sk`), `movieId`, `title` e `genres`.

**`dim_user`** — Dimensão de usuários consolidada a partir das quatro tabelas de origem com usuários (histórico de avaliações, avaliações adicionais, belief data, histórico de recomendações) usando `UNION DISTINCT` para garantir ausência de duplicatas.

**`dim_date`** — Dimensão de datas derivada de todos os timestamps de todas as fontes. Inclui `full_date`, `year`, `month`, `day`, `quarter` e `week`.

**`fact_user_rating`** — Tabela fato unificada de avaliações, combinando `raw_user_rating_history` e `raw_user_additional_rating` em uma única tabela com coluna `rating_source` (`'history'` ou `'additional'`). Associada às três dimensões via chaves surrogate.

**`fact_belief`** — Tabela fato de elicitação de crenças. Armazena `is_seen`, `watch_date`, `user_elicit_rating`, `user_predict_rating`, `system_predict_rating`, `user_certainty`, `month_idx` e `src`. Esta tabela é central para a camada de avaliação do modelo.

**`fact_recommendation`** — Tabela fato de recomendações, armazenando o `predicted_rating` do sistema para cada par usuário-filme em um dado timestamp.

### 5.3 Gold — Views Analíticas (BigQuery)

Nove views analíticas são criadas sobre o modelo dimensional:

| View | Descrição |
|---|---|
| `vw_user_kpis` | KPIs por usuário: total de avaliações, média, desvio padrão, primeira/última atividade, filmes avaliados e vistos |
| `vw_movie_kpis` | KPIs por filme: total de avaliações, média, desvio padrão, primeira/última avaliação |
| `vw_top_10_movies` | Top 10 filmes por média de avaliação (mínimo 50 avaliações) |
| `vw_genre_performance` | Média de avaliação, total de avaliações e total de filmes por gênero (via UNNEST) |
| `vw_ratings_temporal` | Volume mensal de avaliações ao longo do tempo |
| `vw_movies_50_plus` | Todos os filmes com 50+ avaliações, ordenados por volume |
| `vw_recommendation_quality_metrics` | MAE, RMSE, Bias e taxa de acurácia global do sistema de recomendação |
| `vw_recommendation_error_by_genre` | MAE por gênero para o modelo de recomendação |
| `vw_recommendation_error_by_movie` | MAE por filme para o modelo de recomendação |

---

## 6. Modelo de Dados

```
                    ┌──────────────┐
                    │   dim_date   │
                    │──────────────│
                    │ date_sk (PK) │
                    │ full_date    │
                    │ year / month │
                    │ day / quarter│
                    │ week         │
                    └──────┬───────┘
                           │
         ┌─────────────────┼──────────────────┐
         │                 │                  │
         ▼                 ▼                  ▼
┌────────────────┐ ┌────────────────┐ ┌────────────────────┐
│fact_user_rating│ │  fact_belief   │ │ fact_recommendation│
│────────────────│ │────────────────│ │────────────────────│
│ user_sk (FK)   │ │ user_sk (FK)   │ │ user_sk (FK)       │
│ movie_sk (FK)  │ │ movie_sk (FK)  │ │ movie_sk (FK)      │
│ date_sk (FK)   │ │ date_sk (FK)   │ │ date_sk (FK)       │
│ rating         │ │ is_seen        │ │ predicted_rating   │
│ rating_source  │ │ watch_date     │ │ recommendation_    │
│ rating_tstamp  │ │ user_elicit_   │ │ timestamp          │
│                │ │ rating         │ └─────────┬──────────┘
└───────┬────────┘ │ user_predict_  │           │
        │          │ rating         │           │
        │          │ system_predict_│           │
        │          │ rating         │           │
        │          │ user_certainty │           │
        │          │ belief_tstamp  │           │
        │          └───────┬────────┘           │
        │                  │                    │
        └──────────┬───────┘────────────────────┘
                   │
         ┌─────────┴────────┐
         │                  │
         ▼                  ▼
┌──────────────┐  ┌──────────────┐
│   dim_user   │  │   dim_movie  │
│──────────────│  │──────────────│
│ user_sk (PK) │  │ movie_sk (PK)│
│ userId       │  │ movieId      │
└──────────────┘  │ title        │
                  │ genres       │
                  └──────────────┘
```

---

## 7. Qualidade de Dados

Um script abrangente de qualidade de dados (`3_data_quality.sql`) valida todo o modelo em cinco categorias:

**1. Dimensões** — Cada dimensão é verificada quanto a chaves naturais duplicadas, chaves surrogate duplicadas, valores nulos e paridade de contagem de linhas entre tabelas raw e analíticas.

**2. Fatos** — Cada tabela fato é validada quanto a chaves estrangeiras nulas, intervalos de valores válidos (ratings entre 0,5 e 5,0; `is_seen` em {-1, 0, 1}), distribuição por fonte, paridade de contagem de linhas com as tabelas raw e unicidade do grão (sem combinações duplicadas de usuário + filme + timestamp).

**3. Integridade Referencial** — Verificações de cobertura confirmam que todos os usuários, filmes e datas presentes nas tabelas fato existem em suas respectivas tabelas de dimensão. Contagens de registros órfãos são calculadas para as três dimensões.

**4. Regras de Negócio** — Intervalos de avaliação (0,5–5,0), intervalos de belief e intervalos de recomendação são validados explicitamente.

**5. Sanidade Analítica** — Verificações de distribuição de ratings e verificações de volume mensal garantem que os dados se comportam conforme esperado ao longo do tempo.

---

## 8. Dashboard — Metabase

O Metabase roda via Docker e se conecta diretamente ao BigQuery. O dashboard cobre:

- Top 10 filmes por média de avaliação
- Análise de performance por gênero
- Visão geral de KPIs de usuários
- Tendências de volume mensal de avaliações
- Métricas de qualidade do modelo de recomendação (MAE, RMSE, Bias, Taxa de Acurácia)
- Distribuição de erros por gênero e por filme

> **Observação:** O dashboard está hospedado localmente (`http://localhost:3000`). Para embedar ou compartilhar, utilize o signed embedding do Metabase com um token JWT gerado pelo backend. O código de embed nunca deve conter um JWT hardcoded — sempre gere no servidor.

---

## 9. Avaliação do Modelo & Proposta de Negócio

Um dos diferenciais deste projeto é a camada de avaliação do modelo de recomendação, construída sobre a tabela `fact_belief`.

**O que é `fact_belief`?**

Esta tabela registra um estudo de elicitação de crenças onde usuários foram solicitados a prever como avaliariam filmes antes de assisti-los. O sistema também produziu suas próprias predições. Isso permite uma comparação direta entre `system_predict_rating` e `user_predict_rating`.

**Métricas calculadas:**

| Métrica | Fórmula | Significado |
|---|---|---|
| MAE | Média(|sistema − usuário|) | Erro absoluto médio de predição |
| RMSE | √Média((sistema − usuário)²) | Penaliza erros maiores mais fortemente |
| Bias | Média(sistema − usuário) | Tendência sistemática de super/sub-estimação |
| Taxa de Acurácia | % de predições dentro de ±0,5 | Métrica compreensível para negócio |

**Resultados:** As métricas revelaram baixa qualidade de predição — MAE/RMSE elevados e bias significativo — indicando que o modelo de recomendação atual não está performando adequadamente.

**Proposta de Negócio:** Uma apresentação foi produzida para um público não técnico resumindo os achados e propondo uma nova implementação de modelo. A proposta inclui uma recomendação de nova arquitetura de modelo, melhorias esperadas e enquadramento do impacto de negócio, tratando os resultados da avaliação não como um fracasso, mas como uma oportunidade orientada a dados para melhorar a experiência de recomendação.

---

## 10. Como Executar

### Pré-requisitos

- Projeto Google Cloud com BigQuery habilitado
- Bucket GCS com arquivos CSV no prefixo `/bronze/`
- Docker e Docker Compose instalados

### Passos

**1. Criar tabelas externas raw**
```sql
-- Execute: sql/1_create_raw_tables.sql
-- Atualize projeto, dataset e URIs do GCS conforme necessário
```

**2. Construir modelo dimensional**
```sql
-- Execute: sql/2_create_analytical_tables.sql
```

**3. Executar verificações de qualidade de dados**
```sql
-- Execute: sql/3_data_quality.sql
-- Revise todos os resultados antes de prosseguir
```

**4. Criar views Gold**
```sql
-- Execute: sql/4_create_views_gold.sql
```

**5. Iniciar Metabase**
```bash
docker-compose up -d
# Acesso: http://localhost:3000
# Conecte o Metabase ao seu projeto BigQuery
```

---

## 11. Estrutura do Projeto

```
netflix-data-pipeline/
├── README.md                          # Overview do projeto (EN + PT)
├── docs/
│   ├── README_EN.md                   # Documentação completa (Inglês)
│   ├── README_PT.md                   # Este arquivo
│   └── pipeline_documentation.md     # Documento técnico do pipeline
├── sql/
│   ├── 1_create_raw_tables.sql        # Bronze: tabelas externas GCS
│   ├── 2_create_analytical_tables.sql # Silver: dims + fatos
│   ├── 3_data_quality.sql             # Validação de qualidade de dados
│   └── 4_create_views_gold.sql        # Gold: views analíticas
└── docker/
    └── docker-compose.yml             # Container Metabase
```

---

## 12. Referências

- Tutorial original: [Pipeline de Engenharia de Dados com Dados da Netflix (YouTube)](https://www.youtube.com/watch?v=38FhOVq3tI0&t=3046s)
- Documentação do tutorial: [Google Docs](https://docs.google.com/document/d/1FB2CuPPU3fvO7WqRLjbX0qAqOb0E92rvmnUj5gzcR3I/edit?tab=t.a70d0766wb2t)
- [Documentação do BigQuery](https://cloud.google.com/bigquery/docs)
- [Documentação do Metabase](https://www.metabase.com/docs/latest/)
