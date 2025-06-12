# import os
# import re
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, udf
# from pyspark.sql.types import StringType
#
# def clean_text(text):
#     return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))
#
# clean_text_udf = udf(clean_text, StringType())
#
# def clean_text_columns(df):
#     for column in df.columns:
#         if dict(df.dtypes)[column] == 'string':
#             df = df.withColumn(column, clean_text_udf(col(column)))
#     return df
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()
#
#     project_root = "/Users/admin/PycharmProjects/FP_part2"
#     bronze_path = os.path.join(project_root, "data/bronze")
#     silver_path = os.path.join(project_root, "data/silver")
#
#     os.makedirs(silver_path, exist_ok=True)
#
#     tables = ["athlete_bio", "athlete_event_results"]
#
#     for table in tables:
#         input_path = os.path.join(bronze_path, table)
#         output_path = os.path.join(silver_path, table)
#
#         df = spark.read.parquet(input_path)
#         df_cleaned = clean_text_columns(df).dropDuplicates()
#
#         df_cleaned.write.mode("overwrite").parquet(output_path)
#         print(f"[✓] Saved cleaned {table} to {output_path}")
#
#     spark.stop()
# перевірено локально

import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def clean_text_columns(df):
    for column in df.columns:
        if dict(df.dtypes)[column] == 'string':
            df = df.withColumn(column, clean_text_udf(col(column)))
    return df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    dags_dir = os.path.dirname(os.path.abspath(__file__))
    bronze_path = os.path.join(dags_dir, "data", "bronze")
    silver_path = os.path.join(dags_dir, "data", "silver")

    os.makedirs(silver_path, exist_ok=True)

    if not os.path.isdir(bronze_path):
        raise FileNotFoundError(f"Bronze directory not found: {bronze_path}")

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        df = spark.read.parquet(os.path.join(bronze_path, table))

        df_cleaned = clean_text_columns(df).dropDuplicates()

        output_path = os.path.join(silver_path, table)
        os.makedirs(output_path, exist_ok=True)

        df_cleaned.write.mode("overwrite").parquet(output_path)
        print(f"[✓] Saved cleaned {table} to {output_path}")

        df_cleaned.show()

    spark.stop()
