--========================================================
-- User KPI
--========================================================

CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_user_kpis` AS

SELECT
  u.user_sk,

  COUNT(r.rating) AS total_ratings,
  AVG(r.rating) AS avg_rating,
  ROUND(AVG(r.rating), 2) AS avg_rating_round,
  STDDEV(r.rating) AS std_rating,

  MIN(r.rating_timestamp) AS first_activity,
  MAX(r.rating_timestamp) AS last_activity,

  COUNT(DISTINCT r.movie_sk) AS total_movies_rated,

  COUNT(DISTINCT CASE
      WHEN b.is_seen = 1 THEN b.movie_sk
  END) AS total_movies_seen

FROM `netflix-pipeline-amura.netflix_analytical.dim_user` u

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.fact_user_rating` r
  ON u.user_sk = r.user_sk

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.fact_belief` b
  ON u.user_sk = b.user_sk

GROUP BY u.user_sk;

--========================================================
-- Movies KPI
--========================================================

 CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_movie_kpis` AS

SELECT
  m.movie_sk,
  m.title,

  COUNT(f.rating) AS total_ratings,
  AVG(f.rating) AS avg_rating,
  ROUND(AVG(f.rating), 2) AS avg_rating_round,
  STDDEV(f.rating) AS std_rating,

  MIN(f.rating_timestamp) AS first_rating,
  MAX(f.rating_timestamp) AS last_rating

FROM `netflix-pipeline-amura.netflix_analytical.dim_movie` m

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f
  ON m.movie_sk = f.movie_sk

GROUP BY m.movie_sk, m.title;

--========================================================
-- Top 10 movies
--========================================================

CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_top_10_movies` AS

SELECT
  m.title,
  AVG(f.rating) AS avg_rating,
  ROUND(AVG(f.rating), 2) AS avg_rating_round,
  COUNT(*) AS total_ratings

FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f

JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON f.movie_sk = m.movie_sk

GROUP BY m.title
HAVING COUNT(*) > 50
ORDER BY avg_rating DESC
LIMIT 10;

--========================================================
-- Genre KPI
--========================================================

CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_genre_performance` AS

SELECT
  TRIM(genre) AS genre,
  AVG(f.rating) AS avg_rating,
  COUNT(*) AS total_ratings,
  COUNT(DISTINCT m.movie_sk) AS total_movies

FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f

JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON f.movie_sk = m.movie_sk

CROSS JOIN UNNEST(SPLIT(m.genres, '|')) AS genre

GROUP BY genre;

--========================================================
-- Temporal ratings
--========================================================

CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_ratings_temporal` AS

SELECT
  d.year,
  d.month,
  COUNT(*) AS total_ratings

FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f

JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON f.date_sk = d.date_sk

GROUP BY d.year, d.month
ORDER BY d.year, d.month;

--========================================================
-- Movies Ratings 50+
--========================================================

CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_movies_50_plus` AS

SELECT
  m.title,
  COUNT(*) AS total_ratings,
  AVG(f.rating) AS avg_rating,
  ROUND(AVG(f.rating), 2) AS avg_rating_round,
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f

JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON f.movie_sk = m.movie_sk

GROUP BY m.title
HAVING COUNT(*) > 50
ORDER BY total_ratings DESC;

--========================================================
-- Recommendation Quality Metrics
--========================================================
CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_recommendation_quality_metrics` AS

SELECT
  ROUND(AVG(ABS(system_predict_rating - user_predict_rating)), 4) AS mae_global,

  ROUND(SQRT(AVG(POW(system_predict_rating - user_predict_rating, 2))), 4) AS rmse_global,

  ROUND(AVG(system_predict_rating - user_predict_rating), 4) AS bias_global,

  ROUND(
    COUNTIF(ABS(system_predict_rating - user_predict_rating) <= 0.5) / COUNT(*),
    4
  ) AS accuracy_rate,

  COUNT(*) AS total_observations

FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`

WHERE system_predict_rating IS NOT NULL
  AND user_predict_rating IS NOT NULL
  AND user_predict_rating >= 0;
 
--========================================================
-- Error by Genre
--========================================================


 CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_recommendation_error_by_genre` AS

WITH exploded AS (

  SELECT
    ABS(b.system_predict_rating - b.user_predict_rating) AS abs_error,
    genre

  FROM `netflix-pipeline-amura.netflix_analytical.fact_belief` b

  JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
    ON b.movie_sk = m.movie_sk

  CROSS JOIN UNNEST(SPLIT(COALESCE(m.genres, ''), '|')) AS genre

  WHERE b.system_predict_rating IS NOT NULL
    AND b.user_predict_rating >= 0
)

SELECT
  genre,
  ROUND(AVG(abs_error), 4) AS mae_genre,
  COUNT(*) AS total_predictions

FROM exploded

GROUP BY genre
ORDER BY mae_genre DESC;

--========================================================
-- Error by Movie
--========================================================
CREATE OR REPLACE VIEW `netflix-pipeline-amura.netflix_analytical.vw_recommendation_error_by_movie` AS

SELECT
  movie_sk,
  ROUND(AVG(ABS(system_predict_rating - user_predict_rating)), 4) AS mae_movie,
  COUNT(*) AS total_predictions

FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`

WHERE system_predict_rating IS NOT NULL
  AND user_predict_rating >= 0

GROUP BY movie_sk;