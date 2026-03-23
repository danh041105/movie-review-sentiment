import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from pyspark.sql import functions as F
from transformation.common.spark_utils import get_spark_session
from dotenv import load_dotenv
 
load_dotenv()
BRONZE_BUCKET = os.getenv('BRONZE_BUCKET', 'bronze')
SILVER_BUCKET = os.getenv('SILVER_BUCKET', 'silver')
 
def transform_tmdb_movies():
	spark = get_spark_session('TMDB_Transform')
	df = spark.read.json(f's3a://{BRONZE_BUCKET}/tmdb/movies/')
	df = df.select(F.col('raw_payload.*'))
	df = df.dropDuplicates(['id']) \
           .filter(F.col('id').isNotNull()) \
           .filter(F.col('title').isNotNull()) \
           .withColumn('title', F.trim(F.col('title'))) \
           .withColumn('overview', F.trim(F.col('overview'))) \
           .withColumn('vote_average', F.round(F.col('vote_average'), 2))
    df.write.mode('overwrite').parquet(f's3a://{SILVER_BUCKET}/tmdb/movies/')
	print(f'TMDB movies: {df.count()} ban ghi da len Silver!')
	spark.stop()
 
if __name__ == '__main__':
	transform_tmdb_movies()
