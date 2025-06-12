# import os
# import requests
# from pyspark.sql import SparkSession
#
# def download_data(filename, local_folder):
#     url = f"https://ftp.goit.study/neoversity/{filename}.csv"
#     local_path = os.path.join(local_folder, f"{filename}.csv")
#     response = requests.get(url)
#     if response.status_code == 200:
#         with open(local_path, 'wb') as file:
#             file.write(response.content)
#         print(f"[✓] {filename}.csv downloaded to {local_path}")
#     else:
#         raise Exception(f"Failed to download {filename}.csv. Status code: {response.status_code}")
#     return local_path
#
# def save_as_parquet(spark, csv_path, table_name, output_folder):
#     df = spark.read.option("header", True).csv(csv_path)
#     output_path = os.path.join(output_folder, table_name)
#     df.write.mode("overwrite").parquet(output_path)
#     print(f"[✓] Saved {table_name} as Parquet in {output_path}")
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
#
#     project_root = "/Users/admin/PycharmProjects/FP_part2"
#     landing_path = os.path.join(project_root, "data/landing")
#     bronze_path = os.path.join(project_root, "data/bronze")
#
#     os.makedirs(landing_path, exist_ok=True)
#
#     for table in ["athlete_bio", "athlete_event_results"]:
#         csv_file = download_data(table, landing_path)
#         save_as_parquet(spark, csv_file, table, bronze_path)
#
#     spark.stop()
#
# перевірено локально

import os
import requests
from pyspark.sql import SparkSession

def download_data(filename, local_folder):
    url = f"https://ftp.goit.study/neoversity/{filename}.csv"
    os.makedirs(local_folder, exist_ok=True)
    local_path = os.path.join(local_folder, f"{filename}.csv")
    response = requests.get(url)
    if response.status_code == 200:
        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"[✓] {filename}.csv downloaded to {local_path}")
    else:
        raise Exception(f"Failed to download {filename}.csv — status {response.status_code}")
    return local_path

def save_as_parquet(df, table_name, output_folder):
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, table_name)
    df.write.mode("overwrite").parquet(output_path)
    print(f"[✓] Saved {table_name} as Parquet in {output_path}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    dags_dir     = os.path.dirname(os.path.abspath(__file__))
    landing_path = os.path.join(dags_dir, "data", "landing")
    bronze_path  = os.path.join(dags_dir, "data", "bronze")

    for table in ["athlete_bio", "athlete_event_results"]:
        csv_file = download_data(table, landing_path)

        df = spark.read.option("header", True).csv(csv_file)

        save_as_parquet(df, table, bronze_path)

        df.show()

    spark.stop()
