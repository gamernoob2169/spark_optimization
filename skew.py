from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit

spark=(
    SparkSession.builder.appName("SparkSkewPractical")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions","8")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("==============================")
print("Spark Application Created!")
print("==============================")

number_of_records=5_000_000
df=spark.range(number_of_records)
print("Dataset Created!")

skewed_df=df.withColumn(
    "key",
    when(
        col("id")<number_of_records*0.90,
        lit(0)
    ) .otherwise(
        (col("id")%999)+1
    )
)

print("Skewed Dataset Created!")

print("\nKey Distributed:")
skewed_df.groupBy("key").count().orderBy(
    col("count").desc()
).show(10)

print("\nStarting GROUP BY...")

result=(
    skewed_df.groupBy("key").count()
)

result.collect()

print("\nGROUP BY completed!")
print("==============================")
print("open Spark UI:")
print("http://localhost:4040")
print("==============================")

import time
time.sleep(120)
spark.stop()