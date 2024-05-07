from pyspark.sql import SparkSession

# Define a global variable to store the DataFrame
global_df = None

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[4]") \
        .getOrCreate()

    # Path to your CSV file
    data = "C:\\Users\\002E0W744\\Downloads\\bank.csv"

    # Read CSV file and create DataFrame
    global_df = spark.read.csv(data, header=True, inferSchema=True)
    global_df.show()