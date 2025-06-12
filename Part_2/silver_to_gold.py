# import os
# from datetime import datetime
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, current_timestamp
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("SilverToGold").getOrCreate()
#
#     project_root = "/Users/admin/PycharmProjects/FP_part2"
#     silver_path = os.path.join(project_root, "data/silver")
#     gold_path = os.path.join(project_root, "data/gold/avg_stats")
#
#     # Зчитуємо silver-таблиці
#     bio_df = spark.read.parquet(os.path.join(silver_path, "athlete_bio"))
#     event_df = spark.read.parquet(os.path.join(silver_path, "athlete_event_results"))
#
#     # Приводимо height та weight до числового типу
#     bio_df = bio_df.withColumn("height", col("height").cast("float")) \
#                    .withColumn("weight", col("weight").cast("float"))
#
#     # Join
#     joined_df = event_df.join(bio_df, on="athlete_id", how="inner") \
#         .drop(event_df["country_noc"])  # .drop(bio_df["country_noc"])
#
#     # Агрегація
#     agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
#                       .agg(
#                           avg("height").alias("avg_height"),
#                           avg("weight").alias("avg_weight")
#                       ) \
#                       .withColumn("timestamp", current_timestamp())
#
#     # Запис у gold-layer
#     agg_df.write.mode("overwrite").parquet(gold_path)
#
#     print(f"[✓] Saved aggregated data to {gold_path}")
#
#     spark.stop()
# перевірено локально

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    dags_dir = os.path.dirname(os.path.abspath(__file__))
    silver_path = os.path.join(dags_dir, "data", "silver")
    gold_path = os.path.join(dags_dir, "data", "gold", "avg_stats")

    os.makedirs(silver_path, exist_ok=True)
    os.makedirs(gold_path, exist_ok=True)

    bio_df = spark.read.parquet(os.path.join(silver_path, "athlete_bio"))
    event_df = spark.read.parquet(os.path.join(silver_path, "athlete_event_results"))

    bio_df = bio_df.withColumn("height", col("height").cast("float")) \
                   .withColumn("weight", col("weight").cast("float"))

    joined_df = event_df.join(bio_df, on="athlete_id", how="inner") \
                        .drop(event_df["country_noc"])

    agg_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
                      .agg(
                          avg("height").alias("avg_height"),
                          avg("weight").alias("avg_weight")
                      ) \
                      .withColumn("timestamp", current_timestamp())

    agg_df.write.mode("overwrite").parquet(gold_path)
    print(f"[✓] Saved aggregated data to {gold_path}")

    agg_df.show()

    spark.stop()

