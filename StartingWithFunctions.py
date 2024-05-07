from pyspark.sql import SparkSession

# Import the global variable from the first script
from second import global_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[4]") \
        .getOrCreate()

    # Now you can use global_df in your second script
    global_df.show()
