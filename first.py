from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("Hello Spark") \
        .master("local[4]") \
        .getOrCreate()

    # Define schema for DataFrame
    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("marital", StringType(), True),
        # Add more StructFields as needed
    ])

    # Your data
    data = [(24, "job1", "single"),
            (32, "job2", "married"),
            # Add more rows of data
            ]

    # Create DataFrame with explicit schema
    df = spark.createDataFrame(data, schema=schema)

    # Show DataFrame
    df.show()