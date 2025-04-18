from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("KafkaStreamProcessor") \
    .getOrCreate()

schema = StructType() \
    .add("order_id", StringType()) \
    .add("category", StringType()) \
    .add("amount", FloatType())

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sales") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/sales_data") \
        .option("dbtable", "stream_orders") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

agg = parsed.groupBy("category").sum("amount")

start_time = time.time()

query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .trigger(once=True) \
    .start()

query.awaitTermination()

end_time = time.time()
print(f"Stream processing took {end_time - start_time:.2f} seconds")
