from pyspark.sql import SparkSession
from pyspark.sql.types import Row

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Basic Spark Application") \
        .master("local[*]") \
        .getOrCreate()

    df_rows_list = [Row(id=1, name="Max", city="Amsterdam"), Row(id=2, name="Norris", city="Brussels")]

    df = spark.createDataFrame(df_rows_list)

    df.show(2, False)

    spark.stop()

