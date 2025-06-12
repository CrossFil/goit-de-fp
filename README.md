# Final Progect

## Part 2: Building a Batch Data Lake

This project implements a three-layer batch data lake architecture using Apache Spark and Airflow. The data flows from raw ingestion (Landing) through progressively refined layers (Bronze → Silver → Gold).

## Project Layers & Scripts

### 1. Landing Zone
- **Script:** `landing_to_bronze.py`  
- **Responsibilities:**  
  1. Download raw CSV files from FTP (e.g. `https://ftp.goit.study/neoversity/athlete_bio.csv`).  
  2. Read them with Spark (`spark.read.csv(...)`) and write as Parquet to `data/bronze/{table}/`.

### 2. Bronze → Silver
- **Script:** `bronze_to_silver.py`  
- **Responsibilities:**  
  1. Read Parquet from `data/bronze/{table}/`.  
  2. Clean all text columns (remove non-alphanumeric/punctuation chars).  
  3. Deduplicate rows.  
  4. Write cleaned Parquet to `data/silver/{table}/`.

### 3. Silver → Gold
- **Script:** `silver_to_gold.py`  
- **Responsibilities:**  
  1. Read `athlete_bio` and `athlete_event_results` from `data/silver/`.  
  2. Cast `height` and `weight` to numeric types.  
  3. Join on `athlete_id`.  
  4. Group by `sport`, `medal`, `sex`, `country_noc` to compute average height/weight.  
  5. Add a `timestamp` column.  
  6. Write final Parquet to `data/gold/avg_stats/`.

### 4. Orchestration
- **Script:** `project_solution.py` (Airflow DAG)  
- **Responsibilities:**  
  - Define a DAG (`Filonenko_batch_pipeline`) that runs, in order:  
    1. `landing_to_bronze`  
    2. `bronze_to_silver`  
    3. `silver_to_gold`  
  - Use `SparkSubmitOperator` with `application=os.path.join(dags_dir, '<script>.py')`.

---
_All scripts build file paths relative to their own directory (`__file__`), so the entire `Filonenko/` folder (scripts + DAG) can be encrypted and deployed as a single archive in the remote Airflow environment._
