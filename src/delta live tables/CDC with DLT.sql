-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC ## Simplify change data capture with DLT
-- MAGIC
-- MAGIC [Documents](https://docs.databricks.com/aws/en/dlt/cdc)
-- MAGIC
-- MAGIC **Key features:**
-- MAGIC - APPLY CHANGES INTO: Declaratively applies CDC events to a target table.
-- MAGIC - SCD Type 1: Overwrites records (latest state).
-- MAGIC - SCD Type 2: Tracks history with __START_AT and __END_AT columns.
-- MAGIC - Streaming Support: Processes changes incrementally via STREAMING LIVE TABLE.
-- MAGIC - Sequence Handling: Orders changes using a SEQUENCE BY column (e.g., operation_date).
-- MAGIC
-- MAGIC **_Why Use?: Simplifies pipelines, handles out-of-order data, and scales for IoT data._**
-- MAGIC
-- MAGIC
-- MAGIC **Requirements**
-- MAGIC - DLT Pipeline: Run in a Databricks Workflow (Pro/Advanced edition).
-- MAGIC - Source Data: Must include:
-- MAGIC   1. Key column (e.g., device_id).
-- MAGIC   2. Operation column (e.g., operation: INSERT, UPDATE, DELETE).
-- MAGIC   3. Sequence column (e.g., operation_date).
-- MAGIC - Checkpoint Location: Writable path for Auto Loader (e.g., dbfs:/mnt/checkpoints/).
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### SCD type 1
-- MAGIC
-- MAGIC | userId | name    | city       | operation | sequenceNum |
-- MAGIC |--------|---------|------------|-----------|-------------|
-- MAGIC | 124    | Raul    | Oaxaca     | INSERT    | 1           |
-- MAGIC | 123    | Isabel  | Monterrey  | INSERT    | 1           |
-- MAGIC | 125    | Mercedes| Tijuana    | INSERT    | 2           |
-- MAGIC | 126    | Lily    | Cancun     | INSERT    | 2           |
-- MAGIC | 123    | null    | null       | DELETE    | 6           |
-- MAGIC | 125    | Mercedes| Guadalajara| UPDATE    | 6           |
-- MAGIC | 125    | Mercedes| Mexicali   | UPDATE    | 5           |
-- MAGIC | 123    | Isabel  | Chihuahua  | UPDATE    | 5           |
-- MAGIC
-- MAGIC ```sql
-- MAGIC -- Create and populate the target table.
-- MAGIC CREATE OR REFRESH STREAMING TABLE target;
-- MAGIC
-- MAGIC APPLY CHANGES INTO
-- MAGIC   target
-- MAGIC FROM
-- MAGIC   stream(cdc_data.users)
-- MAGIC KEYS
-- MAGIC   (userId)
-- MAGIC APPLY AS DELETE WHEN
-- MAGIC   operation = "DELETE"
-- MAGIC APPLY AS TRUNCATE WHEN
-- MAGIC   operation = "TRUNCATE"
-- MAGIC SEQUENCE BY
-- MAGIC   sequenceNum
-- MAGIC COLUMNS * EXCEPT
-- MAGIC   (operation, sequenceNum)
-- MAGIC STORED AS
-- MAGIC   SCD TYPE 1;
-- MAGIC ```
-- MAGIC
-- MAGIC Output:
-- MAGIC | userId | name    | city       |
-- MAGIC |--------|---------|------------|
-- MAGIC | 124    | Raul    | Oaxaca     |
-- MAGIC | 125    | Mercedes| Guadalajara|
-- MAGIC | 126    | Lily    | Cancun     |
-- MAGIC

-- COMMAND ----------

SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views
-- MAGIC
-- MAGIC - Part of DLT pipelines
-- MAGIC - Not persist in Metastore
-- MAGIC - Used to check the data quality

-- COMMAND ----------

CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;
