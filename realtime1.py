from pyspark.sql import *
from pyspark import SparkConf
from lib.logger import log4J

if __name__ == "__main__":
    conf = SparkConf()
    conf.set("spark.app.name", "Hello Spark")
    conf.set("spark.master","local[3]")
    spark = SparkSession.builder \
        .config(conf=conf)\
        .getOrCreate()
    logger = log4J(spark)
    conf_out = spark.sparkContext.getConf()
    logger.info("starting realtime example")
    logger.info(conf_out.toDebugString())
    logger.info("Finished realtime spark example")
