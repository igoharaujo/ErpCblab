from pyspark.sql import SparkSession

def get_spark_session():
    spark = SparkSession \
            .builder \
            .master("local[1]") \
            .appName("elt_jobs") \
            .config("spark.sql.files.maxPartitionBytes", "10kb") \
            .getOrCreate()
    return spark

