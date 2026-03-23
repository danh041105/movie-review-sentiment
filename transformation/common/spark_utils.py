import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
 
load_dotenv()
 
def get_spark_session(app_name='MovieTransformation'):
	spark = SparkSession.builder \
    	.appName(app_name) \
        .config('spark.hadoop.fs.s3a.endpoint', f'http://{os.getenv("MINIO_ENDPOINT")}') \
        .config('spark.hadoop.fs.s3a.access.key', os.getenv('MINIO_ROOT_USER')) \
        .config('spark.hadoop.fs.s3a.secret.key', os.getenv('MINIO_ROOT_PASSWORD')) \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    	.getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
	return spark
