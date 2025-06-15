# Final Progect

## Part 1. Building an End‑to‑End Streaming Pipeline

This assignment consists of designing and implementing a Spark Structured Streaming solution that continuously ingests athlete competition results from Kafka, enriches them with static bio‑metric data stored in MySQL, computes sport‑specific feature aggregates, and writes the results to both a downstream Kafka topic and back into MySQL. These aggregated features (mean height, mean weight) along with demographic attributes (sex, country) and the target label (medal presence) will be used to train individual ML models for each sport.

## Task description

1. **Ingest athlete bio‑metrics**  
   • Use Spark’s JDBC connector to load the table `olympic_dataset.athlete_bio` from MySQL.  
   • Filter out any records where height or weight is null or not numeric.

2. **Publish event results to Kafka**  
   • Read the MySQL table `athlete_event_results` (historical competition data).  
   • Write each row as a compact JSON message into the Kafka topic `athlete_event_results`.

3. **Consume and parse streaming JSON**  
   • Configure Spark Structured Streaming to consume from the `athlete_event_results` topic.  
   • Parse each incoming JSON message into a DataFrame with explicit columns for all fields (e.g. `athlete_id`, `sport`, `medal`, `timestamp`, etc.).

4. **Join static and streaming data**  
   • Perform a broadcast join of the parsed event results stream with the static athlete bio‑metrics DataFrame on `athlete_id`.  
   • Ensure the join handles late or out‑of‑order events appropriately.

5. **Compute rolling feature aggregates**  
   • For each micro‑batch, group the joined data by `(sport, medal, sex, country_noc)`.  
   • Calculate the average height and average weight for each group.  
   • Append a processing timestamp column (`calculated_at`) indicating when the aggregation occurred.

6. **Write aggregated features to sinks**  
   • Using `forEachBatch`, write each batch of aggregated results to:  
     a. A new Kafka topic `athlete_agg_Filonenko` for downstream streaming consumers  
     b. A MySQL table `athlete_agg_Filonenko` as an append‑only insert
--

All code should be organized into scripts that can be submitted via `spark-submit`, with configuration and schema definitions separated into dedicated modules. The pipeline must support exactly‑once semantics through the use of Spark checkpoints, idempotent Kafka producers, and transactional or idempotent writes to MySQL.  

---

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
