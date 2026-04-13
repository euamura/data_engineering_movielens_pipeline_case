
--=====================================
-- Validation checklist:
-- 1. DIMENSIONS
--
-- 1.1 dim_movie
--   A) Duplicated
--   B) Unique SK
--   C) Non Nulls
--   D) Data Count
--
-- 1.2 dim_user
--   A) Duplicated
--   B) Unique SK
--   C) Non Nulls
--   D) Data Count
--
-- 1.3 dim_date
--   A) Duplicated
--   B) Unique SK
--   C) Non Nulls
--   D) Data Count
--
-- 2. FACTS
--
-- 2.1 fact_user_rating
--   A) Null foreign keys
--   B) Rating range
--   C) Source values
--   D) Data Count
--   E) Duplicated grain
--
-- 2.2 fact_belief
--   A) Null foreign keys
--   B) Null measures
--   C) Valid is_seen
--   D) Valid certainty
--   E) Data Count
--   F) Duplicated grain
--
-- 2.3 fact_recommendation
--   A) Null foreign keys
--   B) Predicted rating range
--   C) Data Count
--   D) Duplicated grain
--
-- 3. INTEGRITY
--
-- 3.1 User coverage
-- 3.2 Movie coverage
-- 3.3 Date coverage
--
-- 4. BUSINESS RULES
--
-- 4.1 Rating valid interval
-- 4.2 Belief valid interval
-- 4.3 Recommendation valid interval
--
-- 5. ANALYTICAL SANITY
--
-- 5.1 Distribution check
-- 5.2 Monthly volume check
--=====================================


--=====================================
-- 1.1 dim_movie validation
--=====================================

-- A) Duplicated movieId
SELECT
  'A_DUPLICATED_movieId' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT movieId
  FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`
  GROUP BY movieId
  HAVING COUNT(*) > 1
);

-- B) Unique movie_sk
SELECT
  'B_DUPLICATED_movie_sk' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT movie_sk
  FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`
  GROUP BY movie_sk
  HAVING COUNT(*) > 1
);

-- C) Non Nulls
SELECT
  'C_NULL_CHECK' AS check_type,
  COUNTIF(movieId IS NULL) AS null_movieId,
  COUNTIF(movie_sk IS NULL) AS null_movie_sk,
  COUNTIF(title IS NULL) AS null_title
FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`;

-- D) Data Count (raw vs dim)
SELECT
  'D_COUNT_COMPARISON' AS check_type,
  (SELECT COUNT(DISTINCT movieId)
   FROM `netflix-pipeline-amura.netflix_raw.raw_movies`) AS raw_distinct,
  (SELECT COUNT(*)
   FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`) AS dim_count;


   --=====================================
-- 1.2 dim_user validation
--=====================================

-- A) Duplicated userId
SELECT
  'A_DUPLICATED_userId' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT userId
  FROM `netflix-pipeline-amura.netflix_analytical.dim_user`
  GROUP BY userId
  HAVING COUNT(*) > 1
);

-- B) Unique user_sk
SELECT
  'B_DUPLICATED_user_sk' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT user_sk
  FROM `netflix-pipeline-amura.netflix_analytical.dim_user`
  GROUP BY user_sk
  HAVING COUNT(*) > 1
);

-- C) Non Nulls
SELECT
  'C_NULL_CHECK' AS check_type,
  COUNTIF(userId IS NULL) AS null_userId,
  COUNTIF(user_sk IS NULL) AS null_user_sk
FROM `netflix-pipeline-amura.netflix_analytical.dim_user`;

-- D) Data Count (all raws vs dim)
SELECT
  'D_COUNT_COMPARISON' AS check_type,

  (
    SELECT COUNT(DISTINCT userId)
    FROM (
      SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`
      UNION DISTINCT
      SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`
      UNION DISTINCT
      SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data`
      UNION DISTINCT
      SELECT userId FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`
    )
  ) AS raw_distinct,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_analytical.dim_user`
  ) AS dim_count;



--=====================================
-- 1.3 dim_date validation
--=====================================

-- A) Duplicated full_date
SELECT
  'A_DUPLICATED_full_date' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT full_date
  FROM `netflix-pipeline-amura.netflix_analytical.dim_date`
  GROUP BY full_date
  HAVING COUNT(*) > 1
);

-- B) Unique date_sk
SELECT
  'B_DUPLICATED_date_sk' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT date_sk
  FROM `netflix-pipeline-amura.netflix_analytical.dim_date`
  GROUP BY date_sk
  HAVING COUNT(*) > 1
);

-- C) Non Nulls
SELECT
  'C_NULL_CHECK' AS check_type,
  COUNTIF(date_sk IS NULL) AS null_date_sk,
  COUNTIF(full_date IS NULL) AS null_full_date
FROM `netflix-pipeline-amura.netflix_analytical.dim_date`;

-- D) Data Count (raw vs dim)
SELECT
  'D_COUNT_COMPARISON' AS check_type,

  (
    SELECT COUNT(DISTINCT DATE(tstamp))
    FROM (
      SELECT tstamp FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`
      UNION ALL
      SELECT tstamp FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`
      UNION ALL
      SELECT tstamp FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data`
      UNION ALL
      SELECT tstamp FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`
      UNION ALL
      SELECT tstamp FROM `netflix-pipeline-amura.netflix_raw.raw_movie_elicitation_set`
    )
  ) AS raw_distinct_dates,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_analytical.dim_date`
  ) AS dim_count;
  

  --=====================================
-- 2.1 fact_user_rating validation
--=====================================

-- A) Null foreign keys
SELECT
  'A_NULL_FOREIGN_KEYS' AS check_type,
  COUNTIF(user_sk IS NULL) AS null_user_sk,
  COUNTIF(movie_sk IS NULL) AS null_movie_sk,
  COUNTIF(date_sk IS NULL) AS null_date_sk
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`;

-- B) Rating range
SELECT
  'B_INVALID_RATING_RANGE' AS check_type,
  COUNT(*) AS issues
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE rating IS NOT NULL
  AND (rating < 0 OR rating > 5);

-- C) rating_source values
SELECT
  'C_INVALID_SOURCE' AS check_type,
  rating_source,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
GROUP BY rating_source;

-- D) Data Count (raw vs fact)
SELECT
  'D_COUNT_COMPARISON' AS check_type,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`
  ) +

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`
  ) AS raw_count,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
  ) AS fact_count;

-- E) Duplicated grain (user + movie + timestamp)
SELECT
  'E_DUPLICATED_GRAIN' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT
    user_sk,
    movie_sk,
    rating_timestamp,
    COUNT(*) AS qtd
  FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
  GROUP BY user_sk, movie_sk, rating_timestamp
  HAVING COUNT(*) > 1
);

--=====================================
-- 2.2 fact_belief validation
--=====================================

-- A) Null foreign keys
SELECT
  'A_NULL_FOREIGN_KEYS' AS check_type,
  COUNTIF(user_sk IS NULL) AS null_user_sk,
  COUNTIF(movie_sk IS NULL) AS null_movie_sk,
  COUNTIF(date_sk IS NULL) AS null_date_sk
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`;

-- B) Null main measures
SELECT
  'B_NULL_MEASURES' AS check_type,
  COUNTIF(user_predict_rating IS NULL) AS null_user_predict_rating,
  COUNTIF(system_predict_rating IS NULL) AS null_system_predict_rating
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`;

-- C) Valid is_seen values
SELECT
  'C_IS_SEEN_VALUES' AS check_type,
  is_seen,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
GROUP BY is_seen;

-- D) Valid certainty values
SELECT
  'D_USER_CERTAINTY_VALUES' AS check_type,
  user_certainty,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
GROUP BY user_certainty
ORDER BY user_certainty;

-- E) Data Count (raw vs fact)
SELECT
  'E_COUNT_COMPARISON' AS check_type,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_raw.raw_belief_data`
  ) AS raw_count,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
  ) AS fact_count;

-- F) Duplicated grain (user + movie + timestamp)
SELECT
  'F_DUPLICATED_GRAIN' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT
    user_sk,
    movie_sk,
    belief_timestamp,
    COUNT(*) AS qtd
  FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
  GROUP BY user_sk, movie_sk, belief_timestamp
  HAVING COUNT(*) > 1
);


--=====================================
-- 2.3 fact_recommendation validation
--=====================================

-- A) Null foreign keys
SELECT
  'A_NULL_FOREIGN_KEYS' AS check_type,
  COUNTIF(user_sk IS NULL) AS null_user_sk,
  COUNTIF(movie_sk IS NULL) AS null_movie_sk,
  COUNTIF(date_sk IS NULL) AS null_date_sk
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`;

-- B) Predicted rating range
SELECT
  'B_INVALID_PREDICTED_RATING' AS check_type,
  COUNT(*) AS issues
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`
WHERE predicted_rating IS NOT NULL
  AND (predicted_rating < 0 OR predicted_rating > 5);

-- C) Data Count (raw vs fact)
SELECT
  'C_COUNT_COMPARISON' AS check_type,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`
  ) AS raw_count,

  (
    SELECT COUNT(*)
    FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`
  ) AS fact_count;

-- D) Duplicated grain (user + movie + timestamp)
SELECT
  'D_DUPLICATED_GRAIN' AS check_type,
  COUNT(*) AS issues
FROM (
  SELECT
    user_sk,
    movie_sk,
    recommendation_timestamp,
    COUNT(*) AS qtd
  FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`
  GROUP BY user_sk, movie_sk, recommendation_timestamp
  HAVING COUNT(*) > 1
);


--=====================================
-- 3.1 user coverage
--=====================================

SELECT
  'user_coverage_rating' AS check_type,
  COUNT(DISTINCT user_sk) AS fact_users,
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_user`) AS dim_users
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`

UNION ALL

SELECT
  'user_coverage_belief',
  COUNT(DISTINCT user_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_user`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`

UNION ALL

SELECT
  'user_coverage_recommendation',
  COUNT(DISTINCT user_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_user`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`;


--=====================================
-- 3.2 movie coverage
--=====================================

SELECT
  'movie_coverage_rating' AS check_type,
  COUNT(DISTINCT movie_sk) AS fact_movies,
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`) AS dim_movies
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`

UNION ALL

SELECT
  'movie_coverage_belief',
  COUNT(DISTINCT movie_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`

UNION ALL

SELECT
  'movie_coverage_recommendation',
  COUNT(DISTINCT movie_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`;


--=====================================
-- 3.3 date coverage
--=====================================

SELECT
  'date_coverage_rating' AS check_type,
  COUNT(DISTINCT date_sk) AS fact_dates,
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_date`) AS dim_dates
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`

UNION ALL

SELECT
  'date_coverage_belief',
  COUNT(DISTINCT date_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_date`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`

UNION ALL

SELECT
  'date_coverage_recommendation',
  COUNT(DISTINCT date_sk),
  (SELECT COUNT(*) FROM `netflix-pipeline-amura.netflix_analytical.dim_date`)
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`;


---------------------------------------------

SELECT
  COUNT(*) AS orphan_users
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE user_sk NOT IN (
  SELECT user_sk FROM `netflix-pipeline-amura.netflix_analytical.dim_user`
);

SELECT
  COUNT(*) AS orphan_movies
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE movie_sk NOT IN (
  SELECT movie_sk FROM `netflix-pipeline-amura.netflix_analytical.dim_movie`
);

SELECT
  COUNT(*) AS orphan_dates
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE date_sk NOT IN (
  SELECT date_sk FROM `netflix-pipeline-amura.netflix_analytical.dim_date`
);


--=====================================
-- 4.1 rating valid interval
--=====================================

SELECT
  'RATING_RANGE_CHECK' AS check_type,
  COUNT(*) AS issues
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE rating IS NOT NULL
  AND (rating < 0.5 OR rating > 5);
----------------------------------
  SELECT
  rating,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating`
WHERE rating IS NOT NULL
  AND (rating < 0 OR rating > 5)
GROUP BY rating
ORDER BY rating;


--=====================================
-- 4.2 belief valid interval
--=====================================

-- user_predict_rating
SELECT
  'USER_PREDICT_RANGE' AS check_type,
  COUNT(*) AS issues
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
WHERE user_predict_rating IS NOT NULL
  AND (user_predict_rating < 0.5 OR user_predict_rating > 5)

UNION ALL

-- system_predict_rating
SELECT
  'SYSTEM_PREDICT_RANGE',
  COUNT(*)
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
WHERE system_predict_rating IS NOT NULL
  AND (system_predict_rating < 0.5 OR system_predict_rating > 5)

UNION ALL

-- is_seen valid values
SELECT
  'IS_SEEN_INVALID',
  COUNT(*)
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
WHERE is_seen NOT IN (-1,0,1);

--=====================================
-- 4.3 recommendation valid interval
--=====================================

SELECT
  'RECOMMENDATION_RANGE_CHECK' AS check_type,
  COUNT(*) AS issues
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`
WHERE predicted_rating IS NOT NULL
  AND (predicted_rating < 0.5 OR predicted_rating > 5);



--=====================================
-- 5.1 Distribution check
--=====================================

-- belief distribution
SELECT
  ROUND(user_predict_rating,1) AS user_prediction,
  ROUND(system_predict_rating,1) AS system_prediction,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_belief`
GROUP BY user_prediction, system_prediction
ORDER BY qtd DESC
LIMIT 20;

-- recommendation distribution 
SELECT
  ROUND(predicted_rating,1) AS predicted_rating,
  COUNT(*) AS qtd
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation`
GROUP BY predicted_rating
ORDER BY predicted_rating;

--=====================================
-- 5.2 Monthly volume check
--=====================================


-- ratings
SELECT
  d.year,
  d.month,
  COUNT(*) AS total_ratings
FROM `netflix-pipeline-amura.netflix_analytical.fact_user_rating` f
JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON f.date_sk = d.date_sk
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- beliefs
SELECT
  d.year,
  d.month,
  COUNT(*) AS total_recommendations
FROM `netflix-pipeline-amura.netflix_analytical.fact_recommendation` f
JOIN `netflix-pipeline-amura.netflix_analytical.dim_date` d
  ON f.date_sk = d.date_sk
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

