def transform_imdb_reviews():
	spark = get_spark_session('IMDB_Reviews_Transform')
	df = spark.read.json(f's3a://{BRONZE_BUCKET}/imdb/reviews/')
	df = df.select(
        F.col('raw_payload.movie_id').alias('movie_id'),
        F.col('raw_payload.author').alias('author'),
        F.col('raw_payload.content').alias('content'),
        F.col('raw_payload.rating').alias('rating'),
        F.col('raw_payload.date').alias('date'),
	)
	df = df.filter(F.col('content').isNotNull()) \
           .filter(F.col('content') != '') \
           .withColumn('content', F.trim(F.col('content'))) \
           .dropDuplicates(['movie_id', 'author'])
    df.write.mode('overwrite').parquet(f's3a://{SILVER_BUCKET}/imdb/reviews/')
	print(f'IMDb reviews: {df.count()} ban ghi da len Silver!')
	spark.stop()
 
if __name__ == '__main__':
	transform_imdb_reviews()
