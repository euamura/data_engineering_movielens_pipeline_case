

##############################################################
## Table Movies
##############################################################

CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_movies`
(
  movieId STRING,
  title STRING,
  genres STRING
)
OPTIONS(
  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/movies.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

##############################################################
## Table User Rating Store
##############################################################


CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_user_rating_history`
(
  userId STRING,
  movieId STRING,
  rating FLOAT64,
  tstamp TIMESTAMP
  

)

OPTIONS(

  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/user_rating_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


##############################################################
## Table User Additional Rating
##############################################################

CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_user_additional_rating`
(
  userId STRING,
  movieId STRING,
  rating FLOAT64,
  tstamp TIMESTAMP
  

)

OPTIONS(

  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/ratings_for_additional_users.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);


##############################################################
## Table Belief Data
##############################################################

CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_belief_data`
(
  userId STRING,
  movieId STRING,
  isSeen INT64,
  watchDate STRING,
  userElicitRatin FLOAT64,
  userPredictRating FLOAT64,
  userCertainty INT64,
  tstamp TIMESTAMP,
  month_idx INT64,
  src INT64,
  systemPredictRating FLOAT64
)

OPTIONS(
  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/belief_data.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

##############################################################
## Movie Elicitation Set
##############################################################

CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_movie_elicitation_set`
(
  movieId STRING,
  month_idx INT64,
  src INT64,
  tstamp TIMESTAMP
)

OPTIONS(
  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/movie_elicitation_set.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

##############################################################
## User Recommendation History
##############################################################
CREATE OR REPLACE EXTERNAL TABLE `netflix-pipeline-amura.netflix_raw.raw_user_recommendation_history`
(
  userId STRING,
  tstamp TIMESTAMP,
  movieId STRING,
  predictedRating FLOAT64
)

OPTIONS(
  format = 'CSV',
  uris = ['gs://netflix-data-video-amura/bronze/user_recommendation_history.csv'],
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);