from pyspark.sql import SparkSession
from pyspark.sql.functions import make_date, when, col, concat_ws
from pyspark.sql.types import ArrayType
import os
from pyspark.sql.functions import explode