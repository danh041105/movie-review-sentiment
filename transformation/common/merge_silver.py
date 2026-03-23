def merge_silver():
	spark = get_spark_session('Merge_Silver')
 
	# Đọc và chuẩn hóa TMDB movies
	tmdb_movies = spark.read.parquet('s3a://' + SILVER_BUCKET + '/tmdb/movies/').select(
        F.col('id').cast('string').alias('movie_id'),
    	F.col('title'),
        F.col('overview').alias('description'),
        F.col('release_date').cast('string'),
        F.col('vote_average').alias('rating'),
    	F.col('vote_count'),
        F.lit('tmdb').alias('source')
	)
 
	# Đọc và chuẩn hóa IMDb movies
	imdb_movies = spark.read.parquet('s3a://' + SILVER_BUCKET + '/imdb/movies/').select(
        F.col('imdb_id').alias('movie_id'),
    	F.col('title'),
    	F.col('description'),
    	F.concat_ws('-', F.col('releaseDate.year').cast('string'),
                        F.col('releaseDate.month').cast('string'),
                        F.col('releaseDate.day').cast('string')).alias('release_date'),
    	F.col('rating'),
    	F.col('vote_count'),
        F.lit('imdb').alias('source')
	)
 
	# Hợp nhất movies
	all_movies = tmdb_movies.unionByName(imdb_movies, allowMissingColumns=True)
                            .dropDuplicates(['movie_id'])
 
	# Hợp nhất reviews (tương tự)
    all_movies.write.mode('overwrite').parquet('s3a://' + SILVER_BUCKET + '/merged/movies/')
    all_reviews.write.mode('overwrite').parquet('s3a://' + SILVER_BUCKET + '/merged/reviews/')
 
	print('Merged movies: ' + str(all_movies.count()) + ' ban ghi!')
	print('Merged reviews: ' + str(all_reviews.count()) + ' ban ghi!')
	spark.stop()
 
if __name__ == '__main__':
	merge_silver()
