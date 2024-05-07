from pyspark.sql import *
from lib.logger import log4J

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Realtime1") \
        .master("local[4]") \
        .getOrCreate()
    logger = log4J(spark)
    logger.info("starting realtime example")
    logger.info("Finished realtime spark example")
