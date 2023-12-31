from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Reading batch JSON file started...")

    spark = SparkSession \
        .builder \
        .appName("Read JSON Data - Batch") \
        .master("local[*]") \
        .getOrCreate()

    batch_df = spark \
        .read \
        .format("json") \
        .load(path="input_data/json")

    batch_df.show(10, False)

    print("Reading batch JSON file completed...")

