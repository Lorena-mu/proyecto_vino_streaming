from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, DoubleType, IntegerType

spark = SparkSession.builder.appName("WineStreamingConsumer").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("fixed_acidity", DoubleType()) \
    .add("volatile_acidity", DoubleType()) \
    .add("citric_acid", DoubleType()) \
    .add("residual_sugar", DoubleType()) \
    .add("density", DoubleType()) \
    .add("alcohol", DoubleType()) \
    .add("quality", IntegerType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "wine_quality") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

stats = parsed.groupBy("quality").agg(avg("alcohol").alias("avg_alcohol"))

query = stats.writeStream.outputMode("complete") \
    .format("console").start()

query.awaitTermination()
