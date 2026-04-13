
--=============================================
-- dim_movie
--=============================================

CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_movie` AS

SELECT
  ROW_NUMBER() OVER(ORDER BY movieId) AS movie_sk,
  movieId,
  title,
  genres
FROM `netflix-pipeline-amura.netflix_raw.raw_movies`;


--=============================================
-- dim_user
--=============================================
CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_user` AS

SELECT
  ROW_NUMBER() OVER(ORDER BY userId) AS user_sk,
  userId
FROM (

  SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`

  UNION DISTINCT

  SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`

  UNION DISTINCT

  SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data`

  UNION DISTINCT

  SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`

);

--=============================================
-- dim_date
--=============================================

CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.dim_date` AS

WITH all_dates AS (

  SELECT DATE(tstamp) AS full_date FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`

  UNION DISTINCT

  SELECT DATE(tstamp) FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`

  UNION DISTINCT

  SELECT DATE(tstamp) FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data`

  UNION DISTINCT

  SELECT DATE(tstamp) FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`

  UNION DISTINCT

  SELECT DATE(tstamp) FROM `netflix-pipeline-amura.netflix_raw.raw_movie_elicitation_set`

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

--=============================================
-- fact_user_rating
--=============================================

CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.fact_user_rating` AS

WITH unified_ratings AS (

  SELECT
    userId,
    movieId,
    SAFE_CAST(rating AS FLOAT64) AS rating,
    tstamp,
    'history' AS rating_source
  FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`

  UNION ALL

  SELECT
    userId,
    movieId,
    SAFE_CAST(rating AS FLOAT64) AS rating,
    tstamp,
    'additional' AS rating_source
  FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`

)

SELECT
  u.user_sk,
  m.movie_sk,
  d.date_sk,
  r.rating,
  r.tstamp AS rating_timestamp,
  r.rating_source

FROM unified_ratings r

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_user` u
  ON r.userId = u.userId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON r.movieId = m.movieId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON DATE(r.tstamp) = d.full_date;

--=============================================
-- fact_belief
--=============================================

CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.fact_belief` AS

SELECT
  u.user_sk,
  m.movie_sk,
  d.date_sk,

  b.isSeen AS is_seen,
  SAFE_CAST(b.watchDate AS DATE) AS watch_date,
  b.userElicitRatin AS user_elicit_rating,
  b.userPredictRating AS user_predict_rating,
  b.userCertainty AS user_certainty,
  b.month_idx,
  b.src,
  b.systemPredictRating AS system_predict_rating,
  b.tstamp AS belief_timestamp

FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data` b

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_user` u
  ON b.userId = u.userId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON b.movieId = m.movieId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON DATE(b.tstamp) = d.full_date;

--=============================================
-- fact_recommendation
--=============================================

CREATE OR REPLACE TABLE `netflix-pipeline-amura.netflix_analytical.fact_recommendation` AS

SELECT
  u.user_sk,
  m.movie_sk,
  d.date_sk,
  r.predictedRating AS predicted_rating,
  r.tstamp AS recommendation_timestamp

FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history` r

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_user` u
  ON r.userId = u.userId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_movie` m
  ON r.movieId = m.movieId

LEFT JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON DATE(r.tstamp) = d.full_date;