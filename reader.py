from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("My App").getOrCreate()
df = spark.read.parquet("part-00000-ae4c7c88-aeae-4100-9e25-08fc7f3ea308-c000.snappy.parquet")
df = df.select("gas")

df.show()
