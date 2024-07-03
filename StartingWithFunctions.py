from pyspark.sql import SparkSession
from data import *

# Import the global variable from the first script
from second import global_df

if __name__ == "__main__":
    #Starting my sparkSession
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[4]") \
        .getOrCreate()
    #importing dataset from data directory
    cleaned_bnb = "data\\Cleaned_airbnb_barcelona.csv"
    df2 = "data\\barcelona_listings.csv"

    #Reading the data with spark into dfs
    airbnb_cleaned = spark.read.csv(cleaned_bnb, header=True, inferSchema=True)
    listings = spark.read.csv(df2, header=True, inferSchema=True)

    #cleaned unwanted array column
    cleaned_df = airbnb_cleaned.drop("_c0")
    #cleaned_df.show()
    # join conditions
    spark.conf.set("spark.sql.shuffle.partitions", 3)
    join_expr = cleaned_df.id == listings.id

    final_df = cleaned_df.join(listings, join_expr, "inner")

    final_df.collect()
    final_df.show()
    input("press a key to stop...")

    # Now you can use global_df in your second script


