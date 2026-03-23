def transform_imdb_movies():
	spark = get_spark_session('IMDB_Transform')
	df = spark.read.json(f's3a://{BRONZE_BUCKET}/imdb/movies/')
	df = df.select(F.col('raw_payload.*'))
	df = df.dropDuplicates(['imdb_id']) \
           .filter(F.col('imdb_id').isNotNull()) \
           .filter(F.col('title').isNotNull()) \
           .withColumn('title', F.trim(F.col('title'))) \
           .withColumn('rating', F.round(F.col('rating'), 2))
    df.write.mode('overwrite').parquet(f's3a://{SILVER_BUCKET}/imdb/movies/')
	print(f'IMDb movies: {df.count()} ban ghi da len Silver!')
	spark.stop()
 
if __name__ == '__main__':
	transform_imdb_movies()
