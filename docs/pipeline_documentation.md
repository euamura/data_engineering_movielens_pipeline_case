# Netflix Data Pipeline — Technical Documentation

> **Project:** Netflix Data Engineering Pipeline  
> **Stack:** Google Cloud Storage · BigQuery · Docker · Metabase  
> **Model:** Star Schema (3 dimensions, 3 fact tables, 9 analytical views)

---

## 1. Overview

This document describes the technical design and implementation of the Netflix Data Pipeline. The pipeline ingests raw CSV data from Google Cloud Storage, transforms it into a dimensional model in BigQuery, validates data quality, exposes analytical views, and serves dashboards via Metabase running in Docker.

The project also includes a model evaluation layer that computes recommendation quality metrics (MAE, RMSE, Bias, Accuracy Rate) and, based on poor results, produced a business proposal for a new model architecture.

---

## 2. Data Flow

```
CSV Files (GCS Bronze)
        │
        ├─ raw_movies
        ├─ raw_user_rating_history
        ├─ raw_user_additional_rating
        ├─ raw_belief_data
        ├─ raw_movie_elicitation_set
        └─ raw_user_recommendation_history
                │
                │  External Table queries 
                ▼
        BigQuery — netflix_raw schema
                │
                │  CREATE OR REPLACE TABLE ... AS SELECT
                ▼
        BigQuery — netflix_analytical schema
                │
                ├─ dim_movie    ◄── raw_movies
                ├─ dim_user     ◄── all 4 user sources (UNION DISTINCT)
                ├─ dim_date     ◄── all 5 timestamp sources (UNION DISTINCT)
                │
                ├─ fact_user_rating    ◄── rating_history + additional_rating
                ├─ fact_belief         ◄── belief_data
                └─ fact_recommendation ◄── recommendation_history
                        │
                        │  CREATE OR REPLACE VIEW
                        ▼
                9 Gold Analytical Views
                        │
                        ▼
                Metabase Dashboards (Docker)
```

---

## 3. Schema: netflix_raw (External Tables)

All six tables are `EXTERNAL TABLE` objects pointing to CSV files on GCS. They use `skip_leading_rows = 1`, `allow_quoted_newlines = TRUE`, and `allow_jagged_rows = TRUE`.

### 3.1 raw_movies

| Column | Type | Description |
|---|---|---|
| movieId | STRING | Original movie identifier |
| title | STRING | Movie title with release year |
| genres | STRING | Pipe-separated genre list (e.g. `Action|Comedy`) |

**Source:** `gs://netflix-data-video-amura/bronze/movies.csv`

### 3.2 raw_user_rating_history

| Column | Type | Description |
|---|---|---|
| userId | STRING | User identifier |
| movieId | STRING | Movie identifier |
| rating | FLOAT64 | Rating value (0.5–5.0 scale) |
| tstamp | TIMESTAMP | Rating timestamp |

**Source:** `gs://netflix-data-video-amura/bronze/user_rating_history.csv`

### 3.3 raw_user_additional_rating

Same schema as `raw_user_rating_history`. Contains ratings from a supplementary cohort of users introduced separately.

**Source:** `gs://netflix-data-video-amura/bronze/ratings_for_additional_users.csv`

### 3.4 raw_belief_data

| Column | Type | Description |
|---|---|---|
| userId | STRING | User identifier |
| movieId | STRING | Movie identifier |
| isSeen | INT64 | Whether user has seen the movie (-1 = unknown, 0 = no, 1 = yes) |
| watchDate | STRING | Date when movie was watched (raw string) |
| userElicitRatin | FLOAT64 | Actual user rating (post-watch, if available) |
| userPredictRating | FLOAT64 | User's predicted rating before watching |
| userCertainty | INT64 | User's confidence in their prediction |
| tstamp | TIMESTAMP | Record timestamp |
| month_idx | INT64 | Elicitation study month index |
| src | INT64 | Source identifier |
| systemPredictRating | FLOAT64 | System's predicted rating for this user-movie pair |

**Source:** `gs://netflix-data-video-amura/bronze/belief_data.csv`

### 3.5 raw_movie_elicitation_set

| Column | Type | Description |
|---|---|---|
| movieId | STRING | Movie identifier |
| month_idx | INT64 | Elicitation study month index |
| src | INT64 | Source identifier |
| tstamp | TIMESTAMP | Record timestamp |

**Source:** `gs://netflix-data-video-amura/bronze/movie_elicitation_set.csv`

### 3.6 raw_user_recommendation_history

| Column | Type | Description |
|---|---|---|
| userId | STRING | User identifier |
| tstamp | TIMESTAMP | Recommendation timestamp |
| movieId | STRING | Recommended movie identifier |
| predictedRating | FLOAT64 | System's predicted rating for the recommendation |

**Source:** `gs://netflix-data-video-amura/bronze/user_recommendation_history.csv`

---

## 4. Schema: netflix_analytical (Dimensional Model)

### 4.1 Dimension Tables

#### dim_movie

```sql
CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_movie` AS
SELECT
  ROW_NUMBER() OVER(ORDER BY movieId) AS movie_sk,
  movieId,
  title,
  genres
FROM `netflix-pipeline-amura.netflix_raw.raw_movies`;
```

| Column | Type | Key | Description |
|---|---|---|---|
| movie_sk | INT64 | PK (surrogate) | Surrogate key generated by ROW_NUMBER |
| movieId | STRING | NK (natural) | Original movie identifier |
| title | STRING | — | Movie title |
| genres | STRING | — | Pipe-separated genre list |

**Design note:** Genres are stored as a pipe-delimited string and split with `UNNEST(SPLIT(..., '|'))` in analytical views to enable per-genre aggregations.

#### dim_user

```sql
CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_user` AS
SELECT
  ROW_NUMBER() OVER(ORDER BY userId) AS user_sk,
  userId
FROM (
  SELECT userId FROM raw_user_rating_history
  UNION DISTINCT
  SELECT userId FROM raw_user_additional_rating
  UNION DISTINCT
  SELECT userId FROM raw_belief_data
  UNION DISTINCT
  SELECT userId FROM raw_user_recommendation_history
);
```

| Column | Type | Key | Description |
|---|---|---|---|
| user_sk | INT64 | PK (surrogate) | Surrogate key |
| userId | STRING | NK (natural) | Original user identifier |

**Design note:** Users are consolidated from all four source tables using `UNION DISTINCT` to ensure a single row per unique user regardless of which source they appear in.

#### dim_date

```sql
CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_date` AS
WITH all_dates AS (
  SELECT DATE(tstamp) AS full_date FROM raw_user_rating_history
  UNION DISTINCT
  SELECT DATE(tstamp) FROM raw_user_additional_rating
  UNION DISTINCT
  SELECT DATE(tstamp) FROM raw_belief_data
  UNION DISTINCT
  SELECT DATE(tstamp) FROM raw_user_recommendation_history
  UNION DISTINCT
  SELECT DATE(tstamp) FROM raw_movie_elicitation_set
)
SELECT
  ROW_NUMBER() OVER(ORDER BY full_date) AS date_sk,
  full_date,
  EXTRACT(YEAR FROM full_date) AS year,
  EXTRACT(MONTH FROM full_date) AS month,
  EXTRACT(DAY FROM full_date) AS day,
  EXTRACT(QUARTER FROM full_date) AS quarter,
  EXTRACT(WEEK FROM full_date) AS week
FROM all_dates;
```

| Column | Type | Key | Description |
|---|---|---|---|
| date_sk | INT64 | PK (surrogate) | Surrogate key |
| full_date | DATE | NK (natural) | Calendar date |
| year | INT64 | — | Year |
| month | INT64 | — | Month (1–12) |
| day | INT64 | — | Day of month |
| quarter | INT64 | — | Quarter (1–4) |
| week | INT64 | — | ISO week number |

**Design note:** The date dimension is derived from all timestamps across all five sources to ensure no orphan dates in any fact table.

---

### 4.2 Fact Tables

#### fact_user_rating

**Grain:** One row per user rating event (user + movie + timestamp).

| Column | Type | Key | Description |
|---|---|---|---|
| user_sk | INT64 | FK → dim_user | User surrogate key |
| movie_sk | INT64 | FK → dim_movie | Movie surrogate key |
| date_sk | INT64 | FK → dim_date | Date surrogate key |
| rating | FLOAT64 | Measure | User rating (0.5–5.0) |
| rating_timestamp | TIMESTAMP | — | Full timestamp of the rating event |
| rating_source | STRING | — | `'history'` or `'additional'` |

**Design note:** Both `raw_user_rating_history` and `raw_user_additional_rating` are unified with `UNION ALL` and tagged with a `rating_source` column. This allows filtering by source while keeping a single table.

#### fact_belief

**Grain:** One row per belief elicitation record (user + movie + timestamp).

| Column | Type | Key | Description |
|---|---|---|---|
| user_sk | INT64 | FK → dim_user | User surrogate key |
| movie_sk | INT64 | FK → dim_movie | Movie surrogate key |
| date_sk | INT64 | FK → dim_date | Date surrogate key |
| is_seen | INT64 | — | Watch status: -1=unknown, 0=not seen, 1=seen |
| watch_date | DATE | — | Date when movie was watched |
| user_elicit_rating | FLOAT64 | Measure | User's actual elicited rating |
| user_predict_rating | FLOAT64 | Measure | User's predicted rating before watching |
| system_predict_rating | FLOAT64 | Measure | System's predicted rating |
| user_certainty | INT64 | — | User confidence level |
| month_idx | INT64 | — | Study month index |
| src | INT64 | — | Source identifier |
| belief_timestamp | TIMESTAMP | — | Record timestamp |

**Design note:** This table is the foundation for all recommendation model evaluation metrics. The comparison between `system_predict_rating` and `user_predict_rating` enables MAE, RMSE, and Bias calculation.

#### fact_recommendation

**Grain:** One row per system recommendation (user + movie + timestamp).

| Column | Type | Key | Description |
|---|---|---|---|
| user_sk | INT64 | FK → dim_user | User surrogate key |
| movie_sk | INT64 | FK → dim_movie | Movie surrogate key |
| date_sk | INT64 | FK → dim_date | Date surrogate key |
| predicted_rating | FLOAT64 | Measure | System's predicted rating |
| recommendation_timestamp | TIMESTAMP | — | Recommendation timestamp |

---

## 5. Schema: netflix_analytical (Gold Views)

### 5.1 vw_user_kpis

Aggregated KPIs per user, joining `fact_user_rating` and `fact_belief`.

**Key metrics:** `total_ratings`, `avg_rating`, `avg_rating_round`, `std_rating`, `first_activity`, `last_activity`, `total_movies_rated`, `total_movies_seen`

### 5.2 vw_movie_kpis

Aggregated KPIs per movie from `fact_user_rating`.

**Key metrics:** `total_ratings`, `avg_rating`, `avg_rating_round`, `std_rating`, `first_rating`, `last_rating`

### 5.3 vw_top_10_movies

Top 10 movies by average rating, requiring at least 50 ratings. Ordered by `avg_rating DESC`, `LIMIT 10`.

### 5.4 vw_genre_performance

Per-genre aggregation using `CROSS JOIN UNNEST(SPLIT(m.genres, '|')) AS genre` to explode the pipe-delimited genres string into individual rows.

**Key metrics:** `avg_rating`, `total_ratings`, `total_movies`

### 5.5 vw_ratings_temporal

Monthly rating volume: `year`, `month`, `total_ratings`. Joined to `dim_date`.

### 5.6 vw_movies_50_plus

All movies with 50+ ratings, sorted by `total_ratings DESC`.

### 5.7 vw_recommendation_quality_metrics

Global recommendation system evaluation from `fact_belief`.

**Filter:** `system_predict_rating IS NOT NULL AND user_predict_rating IS NOT NULL AND user_predict_rating >= 0`

| Metric | SQL Expression |
|---|---|
| MAE | `ROUND(AVG(ABS(system_predict_rating - user_predict_rating)), 4)` |
| RMSE | `ROUND(SQRT(AVG(POW(system_predict_rating - user_predict_rating, 2))), 4)` |
| Bias | `ROUND(AVG(system_predict_rating - user_predict_rating), 4)` |
| Accuracy Rate | `ROUND(COUNTIF(ABS(...) <= 0.5) / COUNT(*), 4)` |

### 5.8 vw_recommendation_error_by_genre

MAE per genre, joining `fact_belief` → `dim_movie` → UNNEST genres.

**Key metrics:** `mae_genre`, `total_predictions`

### 5.9 vw_recommendation_error_by_movie

MAE per movie from `fact_belief`.

**Key metrics:** `mae_movie`, `total_predictions`

---

## 6. Data Quality Framework

The data quality script validates the model across five categories:

### Category 1 — Dimension Integrity

For each dimension (`dim_movie`, `dim_user`, `dim_date`):

| Check | SQL Pattern | Expected |
|---|---|---|
| Duplicate natural key | `GROUP BY natural_key HAVING COUNT(*) > 1` | 0 rows |
| Duplicate surrogate key | `GROUP BY surrogate_key HAVING COUNT(*) > 1` | 0 rows |
| Null values | `COUNTIF(col IS NULL)` | All zeros |
| Row count parity | Raw distinct count vs dim count | Equal |

### Category 2 — Fact Integrity

For each fact table (`fact_user_rating`, `fact_belief`, `fact_recommendation`):

| Check | Description | Expected |
|---|---|---|
| Null FKs | Count null user_sk, movie_sk, date_sk | 0 |
| Rating range | Rating outside [0.5, 5.0] | 0 |
| Source distribution | Count by rating_source | 'history' + 'additional' only |
| Row count parity | Raw count vs fact count | Equal |
| Grain uniqueness | Duplicate (user + movie + timestamp) | 0 |

Additional belief-specific checks:
- `is_seen` must be in {-1, 0, 1}
- `user_certainty` distribution review

### Category 3 — Referential Integrity

Coverage checks: fact users/movies/dates vs dimension counts.  
Orphan checks: fact records pointing to non-existent dimension keys.

### Category 4 — Business Rules

Explicit range validations:
- Ratings: [0.5, 5.0]
- Belief ratings: [0.5, 5.0]
- Recommendation predicted ratings: [0.5, 5.0]
- `is_seen`: {-1, 0, 1}

### Category 5 — Analytical Sanity

- Prediction distribution: joint distribution of `user_predict_rating` × `system_predict_rating`
- Monthly volume trends for ratings and recommendations

---

## 7. Recommendation Model Evaluation

### Methodology

The `fact_belief` table enables a unique model evaluation because it contains both the system's prediction (`system_predict_rating`) and the user's own prediction (`user_predict_rating`) for the same user-movie pairs at the same point in time.

This setup — derived from a belief elicitation study — allows the recommendation error to be measured against user expectations rather than only post-watch ratings, which provides a more realistic estimate of perceived recommendation quality.

### Metrics Definition

**MAE (Mean Absolute Error)**
```
MAE = (1/n) * Σ |system_predict_rating_i - user_predict_rating_i|
```
Average magnitude of prediction errors. Scale: same as the rating scale (0.5–5.0).

**RMSE (Root Mean Square Error)**
```
RMSE = √[(1/n) * Σ (system_predict_rating_i - user_predict_rating_i)²]
```
Like MAE but penalizes large errors more heavily. Always ≥ MAE.

**Bias**
```
Bias = (1/n) * Σ (system_predict_rating_i - user_predict_rating_i)
```
Positive bias = system over-predicts (recommends movies it thinks users will like more than they expect). Negative bias = under-predicts.

**Accuracy Rate**
```
Accuracy Rate = |{i : |system_i - user_i| ≤ 0.5}| / n
```
Fraction of predictions within half a star of the user's expectation. Business-friendly.

### Decomposition Views

Error decomposed by genre (`vw_recommendation_error_by_genre`) and by movie (`vw_recommendation_error_by_movie`) allows identifying which content categories or specific films have higher prediction error — key input for model improvement proposals.

### Findings & Business Proposal

The computed metrics revealed poor model quality across all metrics. This finding was framed not as a technical failure but as a data-informed opportunity. A business presentation was created targeting a non-technical audience with:

- Clear summary of current model performance in business terms
- Explanation of what each metric means for the user experience
- Recommendation for a new model architecture (e.g., collaborative filtering, matrix factorization, or hybrid approach)
- Expected metric improvements and business impact (user engagement, watch rate, recommendation acceptance rate)
- Implementation roadmap

---

## 8. Infrastructure: Docker + Metabase

Metabase is deployed as a Docker container and configured to connect to BigQuery via the official Google BigQuery driver.

### Embedding

Metabase supports signed embedding using JWT tokens. The correct pattern is:

1. Backend generates a JWT signed with `METABASE_SECRET_KEY`
2. JWT payload includes `resource: { dashboard: <id> }`, `params: {}`, and `exp` (expiry)
3. Frontend receives the token via API and passes it to the `<metabase-dashboard>` web component

**Security:** JWT tokens must never be hardcoded in HTML or client-side JavaScript. Always generate server-side with a short expiry (e.g., 10 minutes).

---

## 9. Table Relationship Summary

```
dim_user (1) ──────< fact_user_rating  >────── (1) dim_movie
dim_user (1) ──────< fact_belief       >────── (1) dim_movie
dim_user (1) ──────< fact_recommendation >──── (1) dim_movie
dim_date (1) ──────< fact_user_rating  >
dim_date (1) ──────< fact_belief       >
dim_date (1) ──────< fact_recommendation >
```

All fact-to-dimension relationships use surrogate integer keys (`_sk` suffix). Natural keys (`userId`, `movieId`, `full_date`) are preserved in dimension tables for traceability but foreign keys in fact tables always reference surrogate keys.

---

## 10. Naming Conventions

| Object | Convention | Example |
|---|---|---|
| Raw tables | `raw_<source_name>` | `raw_user_rating_history` |
| Dimensions | `dim_<entity>` | `dim_movie` |
| Facts | `fact_<event>` | `fact_user_rating` |
| Views (Gold) | `vw_<description>` | `vw_top_10_movies` |
| Surrogate keys | `<entity>_sk` | `movie_sk` |
| Natural keys | Original source name | `movieId`, `userId` |
| Timestamps | `<context>_timestamp` | `rating_timestamp` |
| Boolean-like INT | `is_<attribute>` | `is_seen` |
