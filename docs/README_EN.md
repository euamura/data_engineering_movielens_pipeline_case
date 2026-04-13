# Netflix Data Pipeline — Full Documentation (EN)

> **Back to project overview:** [README.md](../README.md) | [Versão em Português](README_PT.md)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture](#2-architecture)
3. [Stack](#3-stack)
4. [Data Sources](#4-data-sources)
5. [Pipeline Layers](#5-pipeline-layers)
   - 5.1 [Bronze — Raw Ingestion (GCS)](#51-bronze--raw-ingestion-gcs)
   - 5.2 [Silver — Dimensional Model (BigQuery)](#52-silver--dimensional-model-bigquery)
   - 5.3 [Gold — Analytical Views (BigQuery)](#53-gold--analytical-views-bigquery)
6. [Data Model](#6-data-model)
7. [Data Quality](#7-data-quality)
8. [Dashboard — Metabase](#8-dashboard--metabase)
9. [Model Evaluation & Business Proposal](#9-model-evaluation--business-proposal)
10. [How to Run](#10-how-to-run)
11. [Project Structure](#11-project-structure)
12. [References](#12-references)

---

## 1. Project Overview

This project implements a complete **data engineering pipeline** using real Netflix recommendation datasets. It was built following a YouTube tutorial as a starting point and then extended with additional analytical layers, custom dimensional modeling, data quality checks, and a business-oriented model evaluation analysis.

The pipeline goes from raw CSV files stored in Google Cloud Storage all the way to interactive dashboards in Metabase, passing through a well-structured dimensional model in BigQuery. Additionally, recommendation quality metrics (MAE, RMSE, Bias) were computed, and the results — which were poor — motivated a business presentation proposing a new model architecture.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (CSV)                       │
│  movies | user_rating_history | additional_ratings |            │
│  belief_data | movie_elicitation_set | recommendation_history   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│             BRONZE — Google Cloud Storage (GCS)                 │
│        gs://netflix-data-video-amura/bronze/*.csv               │
└──────────────────────────┬──────────────────────────────────────┘
                           │  External Tables
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│             SILVER — BigQuery (netflix_raw)                     │
│   raw_movies | raw_user_rating_history |                        │
│   raw_user_additional_rating | raw_belief_data |                │
│   raw_movie_elicitation_set | raw_user_recommendation_history   │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Dimensional Modeling
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│          SILVER — BigQuery (netflix_analytical)                 │
│   dim_movie | dim_user | dim_date                               │
│   fact_user_rating | fact_belief | fact_recommendation          │
└──────────────────────────┬──────────────────────────────────────┘
                           │  Analytical Views
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│           GOLD — BigQuery Views (netflix_analytical)            │
│   vw_user_kpis | vw_movie_kpis | vw_top_10_movies              │
│   vw_genre_performance | vw_ratings_temporal |                  │
│   vw_movies_50_plus | vw_recommendation_quality_metrics |       │
│   vw_recommendation_error_by_genre | vw_recommendation_error_by_movie │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                   METABASE (Docker)                             │
│           Business Dashboards + Model Evaluation                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Stack

| Layer | Technology |
|---|---|
| Raw Storage | Google Cloud Storage (GCS) |
| Data Warehouse | Google BigQuery |
| Containerization | Docker |
| Business Intelligence | Metabase |
| SQL Dialect | BigQuery Standard SQL |

---

## 4. Data Sources

All files are stored as CSVs in the GCS bucket `gs://netflix-data-video-amura/bronze/`.

| File | Description |
|---|---|
| `movies.csv` | Movie catalog: movieId, title, genres (pipe-separated) |
| `user_rating_history.csv` | Historical user ratings: userId, movieId, rating, tstamp |
| `ratings_for_additional_users.csv` | Supplementary ratings for a new user cohort |
| `belief_data.csv` | Belief elicitation study: user and system predictions, certainty, watch status |
| `movie_elicitation_set.csv` | Movies selected for the elicitation study |
| `user_recommendation_history.csv` | System-generated recommendations with predicted ratings |

---

## 5. Pipeline Layers

### 5.1 Bronze — Raw Ingestion (GCS)

Raw CSV files are deposited in the GCS bucket. No transformation happens at this stage — it is the single source of truth for all downstream processing.

### 5.2 Silver — Dimensional Model (BigQuery)

**External Tables (netflix_raw schema)**

BigQuery external tables are created pointing directly to the GCS CSVs. This avoids data duplication while enabling SQL querying over raw files.

**Dimensional Tables (netflix_analytical schema)**

Three dimension tables and three fact tables are built:

**`dim_movie`** — Movie dimension with surrogate key (`movie_sk`), `movieId`, `title`, and `genres`.

**`dim_user`** — User dimension consolidated from all four user-facing source tables (rating history, additional ratings, belief data, recommendation history) using `UNION DISTINCT` to ensure no duplicate users.

**`dim_date`** — Date dimension derived from all timestamps across all sources. Includes `full_date`, `year`, `month`, `day`, `quarter`, and `week`.

**`fact_user_rating`** — Unified rating fact table combining both `raw_user_rating_history` and `raw_user_additional_rating` into a single table with a `rating_source` column (`'history'` or `'additional'`). Joined to all three dimensions via surrogate keys.

**`fact_belief`** — Belief elicitation fact table. Stores `is_seen`, `watch_date`, `user_elicit_rating`, `user_predict_rating`, `system_predict_rating`, `user_certainty`, `month_idx`, and `src`. This table is central to the model evaluation layer.

**`fact_recommendation`** — Recommendation fact table storing the system's `predicted_rating` for each user-movie pair at a given timestamp.

### 5.3 Gold — Analytical Views (BigQuery)

Nine analytical views are created on top of the dimensional model:

| View | Description |
|---|---|
| `vw_user_kpis` | Per-user KPIs: total ratings, avg rating, stddev, first/last activity, distinct movies rated and seen |
| `vw_movie_kpis` | Per-movie KPIs: total ratings, avg rating, stddev, first/last rating |
| `vw_top_10_movies` | Top 10 movies by average rating (min. 50 ratings) |
| `vw_genre_performance` | Average rating, total ratings, and total movies per genre (via UNNEST) |
| `vw_ratings_temporal` | Monthly rating volume over time |
| `vw_movies_50_plus` | All movies with 50+ ratings, sorted by volume |
| `vw_recommendation_quality_metrics` | Global MAE, RMSE, Bias, and accuracy rate for the recommender system |
| `vw_recommendation_error_by_genre` | MAE per genre for the recommendation model |
| `vw_recommendation_error_by_movie` | MAE per movie for the recommendation model |

---

## 6. Data Model

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

## 7. Data Quality

A comprehensive data quality script (`3_data_quality.sql`) validates the entire model across five categories:

**1. Dimensions** — Each dimension is checked for duplicated natural keys, duplicated surrogate keys, null values, and row count parity between raw and analytical tables.

**2. Facts** — Each fact table is validated for null foreign keys, valid value ranges (ratings between 0.5–5.0, `is_seen` in {-1, 0, 1}), source distribution, row count parity with raw tables, and grain uniqueness (no duplicate user + movie + timestamp combinations).

**3. Referential Integrity** — Coverage checks confirm that all users, movies, and dates present in the fact tables exist in their respective dimension tables. Orphan record counts are computed for all three dimensions.

**4. Business Rules** — Rating ranges (0.5–5.0), belief intervals, and recommendation intervals are validated explicitly.

**5. Analytical Sanity** — Rating distribution checks and monthly volume checks ensure the data behaves as expected across time.

---

## 8. Dashboard — Metabase

Metabase runs via Docker and connects directly to BigQuery. The dashboard covers:

- Top 10 movies by average rating
- Genre performance analysis
- User KPI overview
- Monthly rating volume trends
- Recommendation quality metrics (MAE, RMSE, Bias, Accuracy Rate)
- Error distribution by genre and by movie

> **Note:** The dashboard is hosted locally (`http://localhost:3000`). To embed or share, use Metabase's signed embedding with a backend-generated JWT token. The embed code should never contain a hardcoded JWT — always generate it server-side.

---

## 9. Model Evaluation & Business Proposal

One of the differentiating aspects of this project is the recommendation model evaluation layer, built on the `fact_belief` table.

**What is `fact_belief`?**

This table records a belief elicitation study where users were asked to predict how they would rate movies before watching them. The system also produced its own predictions. This enables a direct comparison between `system_predict_rating` and `user_predict_rating`.

**Metrics computed:**

| Metric | Formula | Meaning |
|---|---|---|
| MAE | Avg(|system − user|) | Average absolute prediction error |
| RMSE | √Avg((system − user)²) | Penalizes large errors more |
| Bias | Avg(system − user) | Systematic over/under-prediction |
| Accuracy Rate | % of predictions within ±0.5 | Business-friendly metric |

**Results:** The metrics revealed poor prediction quality — high MAE/RMSE and meaningful bias — indicating the current recommendation model is not performing adequately.

**Business Proposal:** A presentation was produced for a non-technical audience summarizing the findings and proposing a new model implementation. The proposal includes a new model architecture recommendation, expected improvements, and business impact framing, treating the evaluation results not as a failure but as a data-driven opportunity to improve the recommendation experience.

---

## 10. How to Run

### Prerequisites

- Google Cloud project with BigQuery enabled
- GCS bucket with CSV files in the `/bronze/` prefix
- Docker and Docker Compose installed

### Steps

**1. Create raw external tables**
```sql
-- Run: sql/1_create_raw_tables.sql
-- Update project, dataset, and GCS URIs as needed
```

**2. Build dimensional model**
```sql
-- Run: sql/2_create_analytical_tables.sql
```

**3. Run data quality checks**
```sql
-- Run: sql/3_data_quality.sql
-- Review all results before proceeding
```

**4. Create Gold views**
```sql
-- Run: sql/4_create_views_gold.sql
```

**5. Start Metabase**
```bash
docker-compose up -d
# Access: http://localhost:3000
# Connect Metabase to your BigQuery project
```

---

## 11. Project Structure

```
netflix-data-pipeline/
├── README.md                          # Project overview (EN + PT)
├── docs/
│   ├── README_EN.md                   # This file
│   ├── README_PT.md                   # Full documentation (Portuguese)
│   └── pipeline_documentation.md     # Technical pipeline document
├── sql/
│   ├── 1_create_raw_tables.sql        # Bronze: GCS external tables
│   ├── 2_create_analytical_tables.sql # Silver: dims + facts
│   ├── 3_data_quality.sql             # Data quality validation
│   └── 4_create_views_gold.sql        # Gold: analytical views
└── docker/
    └── docker-compose.yml             # Metabase container
```

---

## 12. References

- Original tutorial: [Data Engineering Pipeline with Netflix Data (YouTube)](https://www.youtube.com/watch?v=38FhOVq3tI0&t=3046s)
- Tutorial documentation: [Google Docs](https://docs.google.com/document/d/1FB2CuPPU3fvO7WqRLjbX0qAqOb0E92rvmnUj5gzcR3I/edit?tab=t.a70d0766wb2t)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Metabase Documentation](https://www.metabase.com/docs/latest/)
