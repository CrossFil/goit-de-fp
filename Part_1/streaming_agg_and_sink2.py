
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, isnan, avg, to_json, struct
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)
import sys
import time

# Параметри підключення
kafka_bootstrap_servers = "77.81.230.104:9092"
input_topic = "athlete_event_results"
output_topic = "athlete_agg_Filonenko"

# Параметри MySQL
jdbc_url = "jdbc:mysql://217.61.57.46:3306/neo_data"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
jdbc_driver = "com.mysql.cj.jdbc.Driver"
bio_table = "olympic_dataset.athlete_bio"
agg_table = "olympic_dataset.athlete_agg_Filonenko"

# SparkSession
spark = SparkSession.builder \
    .appName("AthleteStreamingAggregation") \
    .config("spark.jars", "/Users/admin/PycharmProjects/FP_part1/mysql-connector-j-8.0.32.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Схема JSON повідомлення з ВХІДНОГО Kafka-топіку (athlete_event_results)
input_kafka_schema = StructType([
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("event", StringType(), True),
    StructField("isTeamSport", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("pos", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("sport", StringType(), True)
])

# Етап 3: Зчитати дані з результатами змагань з Kafka-топіку.
raw_stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.group.id", f"athlete_event_group_filonenko_{int(time.time())}") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("kafka.request.timeout.ms", "60000") \
    .option("kafka.session.timeout.ms", "60000") \
    .option("kafka.max.poll.interval.ms", "300000") \
    .option("maxOffsetsPerTrigger", "500") \
    .option("failOnDataLoss", "false") \
    .load()

# Етап 5: Зробити певні трансформації в даних (Парсинг JSON).
parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), input_kafka_schema).alias("data")) \
    .select("data.*")

# Етап 1: Зчитати дані фізичних показників атлетів з MySQL таблиці.
bio_df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("driver", jdbc_driver) \
    .option("dbtable", bio_table) \
    .option("user", jdbc_user) \
    .option("password", jdbc_password) \
    .load() \
    .select("athlete_id", "sex",
            col("height").cast(DoubleType()).alias("height"),
            col("weight").cast(DoubleType()).alias("weight"))

# ДІАГНОСТИЧНИЙ ВИВІД bio_df
print("\n--- Diagnostic: Sample of bio_df from MySQL (After Explicit Cast) ---")
bio_df.printSchema()
bio_df.show(5, truncate=False)
print("-----------------------------------------------\n")

# Етап 4: Об’єднати дані з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці.
joined_stream = parsed_stream.join(bio_df, on="athlete_id", how="left")

# Етап 2: Відфільтрувати дані.
filtered_stream = joined_stream.filter(
    col("height").isNotNull() & ~isnan(col("height")) &
    col("weight").isNotNull() & ~isnan(col("weight")) &
    col("sex").isNotNull() &
    (col("medal") != "nan")
)

# Етап 5: Зробити певні трансформації в даних (Агрегація).
agg_df = filtered_stream.groupBy("sport", "medal", "sex", "country_noc").agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("calculation_timestamp")
)

# ДІАГНОСТИЧНИЙ ВИВІД АГРЕГОВАНИХ ДАНИХ (для перевірки агрегації)
console_query = agg_df.writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint_athlete_agg_filonenko_console_diag") \
    .trigger(processingTime="10 seconds") \
    .start()


# Етап 6: Cтрим даних у Kafka та MySQL.
def foreach_batch_function(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"Batch {batch_id}: Number of records in batch_df before writing: {batch_df.count()}")
        # Запис у вихідний Kafka-топік (6.а)
        # Перетворюємо DataFrame в JSON рядок для запису в Kafka
        try:
            batch_df.select(to_json(struct(col("*"))).alias("value")) \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
                .option("topic", output_topic) \
                .option("kafka.security.protocol", "SASL_PLAINTEXT") \
                .option("kafka.sasl.mechanism", "PLAIN") \
                .option("kafka.sasl.jaas.config",
                        'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
                .save()
            print(f"Batch {batch_id}: Aggregated data written to Kafka topic '{output_topic}'")
        except Exception as e:
            print(f"Error writing to Kafka in batch {batch_id}: {e}")

        # Запис у базу даних MySQL (6.б)
        try:
            batch_df.write.format("jdbc").options(
                url=jdbc_url,
                driver=jdbc_driver,
                dbtable=agg_table,
                user=jdbc_user,
                password=jdbc_password
            ).mode("append").save()
            print(f"Batch {batch_id}: Aggregated data written to MySQL table '{agg_table}'")
        except Exception as e:
            print(f"Error writing to MySQL in batch {batch_id}: {e}")

# Запуск основного стріму (запис у Kafka та MySQL)
main_query = agg_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoint_athlete_agg_filonenko_main") \
    .trigger(processingTime="30 seconds") \
    .start()

# Очікуємо завершення обох запитів
console_query.awaitTermination()
main_query.awaitTermination()