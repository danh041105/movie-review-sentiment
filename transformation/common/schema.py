from pyspark.sql.types import *
 
TMDB_MOVIE_SCHEMA = StructType([
	StructField('id', IntegerType(), True),
	StructField('title', StringType(), True),
	StructField('overview', StringType(), True),
    StructField('release_date', StringType(), True),
    StructField('vote_average', DoubleType(), True),
	StructField('vote_count', IntegerType(), True),
	StructField('popularity', DoubleType(), True),
	StructField('genres', ArrayType(StringType()), True),
])
 
TMDB_REVIEW_SCHEMA = StructType([
	StructField('movie_id', IntegerType(), True),
	StructField('author', StringType(), True),
	StructField('content', StringType(), True),
	StructField('rating', DoubleType(), True),
	StructField('created_at', StringType(), True),
])
 
IMDB_MOVIE_SCHEMA = StructType([
	StructField('imdb_id', StringType(), True),
	StructField('title', StringType(), True),
    StructField('releaseDate', StringType(), True),
	StructField('genres', ArrayType(StringType()), True),
	StructField('rating', DoubleType(), True),
	StructField('vote_count', IntegerType(), True),
])
