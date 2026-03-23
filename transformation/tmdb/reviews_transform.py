def transform_tmdb_reviews():
	spark = get_spark_session('TMDB_Reviews_Transform')
	df = spark.read.json(f's3a://{BRONZE_BUCKET}/tmdb/reviews/')
	df = df.select(
        F.col('metadata.search_parameters.movie_id').alias('movie_id'),
        F.col('raw_payload.author').alias('author'),
        F.col('raw_payload.content').alias('content'),
        F.col('raw_payload.author_details.rating').alias('rating'),
	)
	df = df.filter(F.col('content').isNotNull()) \
           .filter(F.col('content') != '') \
           .withColumn('content', F.trim(F.col('content'))) \
           .dropDuplicates(['movie_id', 'author'])
    df.write.mode('overwrite').parquet(f's3a://{SILVER_BUCKET}/tmdb/reviews/')
	print(f'TMDB reviews: {df.count()} ban ghi da len Silver!')
	spark.stop()
 
if __name__ == '__main__':
	transform_tmdb_reviews()
