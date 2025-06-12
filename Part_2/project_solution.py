from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='Filonenko_batch_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['Filonenko'],
) as dag:

    dags_dir = os.path.dirname(os.path.abspath(__file__))

    landing_to_bronze = SparkSubmitOperator(
        task_id='landing_to_bronze',
        conn_id='spark-default',
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        verbose=1,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        conn_id='spark-default',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        verbose=1,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='silver_to_gold',
        conn_id='spark-default',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        verbose=1,
    )

    landing_to_bronze >> bronze_to_silver >> silver_to_gold
