# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create customers table
# MAGIC CREATE TABLE customers (
# MAGIC   customer_id INT PRIMARY KEY,
# MAGIC   email STRING,
# MAGIC   profile STRING,
# MAGIC   updated TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create books table
# MAGIC CREATE TABLE books (
# MAGIC   book_id INT PRIMARY KEY,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Create orders table
# MAGIC CREATE TABLE orders (
# MAGIC   order_id INT PRIMARY KEY,
# MAGIC   timestamp TIMESTAMP,
# MAGIC   customer_id INT,
# MAGIC   quantity INT,
# MAGIC   total DOUBLE,
# MAGIC   books ARRAY<INT>  -- list of book_ids
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC
# MAGIC -- Insert into customers
# MAGIC INSERT INTO customers (customer_id, email, profile, updated) VALUES
# MAGIC   (1, 'alice@example.com', 'Regular customer', current_timestamp()),
# MAGIC   (2, 'bob@example.com', 'Premium member', current_timestamp()),
# MAGIC   (3, 'charlie@example.com', 'New customer', current_timestamp());
# MAGIC
# MAGIC -- Insert into books
# MAGIC INSERT INTO books (book_id, title, author, category, price) VALUES
# MAGIC   (101, 'The Great Gatsby', 'F. Scott Fitzgerald', 'Fiction', 10.99),
# MAGIC   (102, '1984', 'George Orwell', 'Fiction', 8.99),
# MAGIC   (103, 'A Brief History of Time', 'Stephen Hawking', 'Science', 15.49),
# MAGIC   (104, 'The Art of War', 'Sun Tzu', 'Philosophy', 6.99);
# MAGIC
# MAGIC -- Insert into orders
# MAGIC INSERT INTO orders (order_id, timestamp, customer_id, quantity, total, books) VALUES
# MAGIC   (1001, current_timestamp(), 1, 2, 19.98, array(101, 102)),
# MAGIC   (1002, current_timestamp(), 2, 1, 15.49, array(103)),
# MAGIC   (1003, current_timestamp(), 3, 3, 26.97, array(104, 101, 102));
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Reading Stream

# COMMAND ----------

query_read_stream = (spark.readStream
      .table("books")
      .createOrReplaceTempView("default.books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM books_streaming_tmp_vw;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

query = (spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### STOP STREAMING

# COMMAND ----------

spark.streams.active

# COMMAND ----------

query.status   # shows trigger status
query.lastProgress  # shows metrics of the last trigger (e.g., input rows, duration)

# See if stream is active
query.isActive
