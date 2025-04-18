from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType
import time

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
    .option("startingOffsets", "earliest") \
    .load()

print("\n✅ Kafka stream configured.")
print("✔️  Kafka schema:")
df.printSchema()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

print("\n✅ Data schema after parsing JSON:")
parsed.printSchema()

# print("============= ABOUT TO START =========================\n")
# parsed.show()

def write_to_postgres(batch_df, batch_id):
    print("============= ABOUT TO WRITE =========================\n")
    print(f"\n🔄 Writing batch {batch_id} to PostgreSQL with {batch_df.count()} rows")

    # try:
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/sales_data") \
        .option("dbtable", "stream_orders") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print(f"✅ Batch {batch_id} written successfully.")
    # except Exception as e:
    # print(f"❌ Error while writing batch {batch_id}:", e)


agg = parsed.groupBy("category").sum("amount")

start_time = time.time()
print("============= ABOUT TO START =========================\n")
query = parsed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()
    # .trigger(once=True) \

try:
    query = parsed.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()

except Exception as e:
    print("❌ Stream query failed to start or crashed:", e)

end_time = time.time()
print(f"Stream processing took {end_time - start_time:.2f} seconds")
