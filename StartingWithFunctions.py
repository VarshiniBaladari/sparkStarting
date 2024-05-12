from pyspark.sql import SparkSession
from data import *

# Import the global variable from the first script
from second import global_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[4]") \
        .getOrCreate()
    cleaned_bnb = "data\\Cleaned_airbnb_barcelona.csv"
    df2 = "data\\barcelona_listings.csv"
    airbnb_cleaned = spark.read.csv(cleaned_bnb, header=True, inferSchema=True)
    listings = spark.read.csv(df2, header=True, inferSchema=True)

    # Now you can use global_df in your second script
    listings.show()

