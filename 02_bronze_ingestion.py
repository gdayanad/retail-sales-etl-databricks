# Databricks notebook source
# ===========================================
# NOTEBOOK: 02_bronze_ingestion
# PURPOSE:  Read raw files from landing zone
#           and save as Bronze Delta tables
# AUTHOR:   Gayatri Dayanand
# ===========================================

# First let's confirm our files are accessible
# by reading just the first few lines of orders.csv

# spark is the main entry point for all data operations in Databricks
# it's automatically available in every notebook - you never need to create it
# spark.read means "I want to read some data"
# .format("csv") means "the file is a CSV"
# .option("header", "true") means "first row is column names, not data"
# .option("inferSchema", "true") means "figure out data types automatically"
# .load(...) means "here's where the file is"

df_orders_raw = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/retail_data/landing/orders/orders.csv")
)

# display() shows the data as a nice table
# it's a Databricks-specific command - doesn't work outside Databricks
display(df_orders_raw)

# COMMAND ----------

# printSchema() shows the structure of your DataFrame
# it tells you each column name and what data type Spark assigned it
# StringType  = text
# IntegerType = whole number
# DoubleType  = decimal number

df_orders_raw.printSchema()

# COMMAND ----------

# withColumn() adds a new column to the DataFrame
# We're adding two extra columns that didn't exist in the CSV:
#
# _ingest_at : the exact timestamp when WE loaded this data
#              useful for debugging - "when did this row arrive?"
#
# _source    : a label showing where this data came from
#              useful when you have multiple sources feeding one table
#
# current_timestamp() is a Spark function that returns right now as a timestamp
# lit("orders_csv") means "literal value" - just a fixed text label

from pyspark.sql.functions import current_timestamp, lit

df_orders_bronze = (df_orders_raw
    .withColumn("_ingest_at", current_timestamp())
    .withColumn("_source", lit("orders_csv"))
)

display(df_orders_bronze)

# COMMAND ----------

# .write means "I want to save this DataFrame somewhere"
# .format("delta") means "save as a Delta table, not just a CSV"
# .mode("overwrite") means "if this table already exists, replace it"
#    other options are:
#    "append"    → add rows to existing table
#    "ignore"    → do nothing if table exists
#    "errorifexists" → crash if table already exists
# .saveAsTable("workspace.default.bronze_orders") means:
#    workspace  = catalog (your top level)
#    default    = schema (like a folder)
#    bronze_orders = the table name

(df_orders_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_orders")
)

print("bronze_orders table created ✓")

# COMMAND ----------

# spark.sql() lets you write regular SQL inside Python
# This is one of the best things about Databricks -
# you can switch between Python and SQL freely
# SELECT * means "give me all columns"
# LIMIT 5 means "only show 5 rows" - good habit, 
# never SELECT * without LIMIT on big tables

display(spark.sql("SELECT * FROM workspace.default.bronze_orders LIMIT 5"))

# COMMAND ----------

# Reading customers JSON file
# .format("json") tells Spark this is a JSON file
# multiLine="true" means the JSON spans multiple lines
# (our file has one big array, not one JSON object per line)

from pyspark.sql.functions import current_timestamp, lit

df_customers_raw = (spark.read
    .format("json")
    .option("multiLine", "true")
    .load("/Volumes/workspace/default/retail_data/landing/customers/customers.json")
)

# Add bronze metadata columns
df_customers_bronze = (df_customers_raw
    .withColumn("_ingest_at", current_timestamp())
    .withColumn("_source", lit("customers_json"))
)

# Save as Delta table
(df_customers_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_customers")
)

# (df_customers_bronze      # ← start with this DataFrame in memory
#     .write                # ← I want to WRITE (save) this data
#     .format("delta")      # ← save it in Delta format
#     .mode("overwrite")    # ← if table exists, replace it
#     .saveAsTable("workspace.default.bronze_customers")  # ← save here data moves from MEMORY → DISK table is registered in catalog -- table now exists permanently in catalog even if you close Databricks, it's still there
# )

# # saves to catalog — queryable by name ✅
# .saveAsTable("workspace.default.bronze_customers")

# # saves to a path — NOT in catalog ❌
# .save("/Volumes/workspace/default/retail_data/bronze/customers")
# We always use saveAsTable() so tables appear in the catalog and can be queried easily with spark.table() or SQL.


print("bronze_customers table created ✓")
display(df_customers_bronze)

# COMMAND ----------

# Products is a CSV just like orders
# same pattern, different file path and table name

from pyspark.sql.functions import current_timestamp, lit

df_products_raw = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Volumes/workspace/default/retail_data/landing/products/products.csv")
)

# Add bronze metadata columns
df_products_bronze = (df_products_raw
    .withColumn("_ingest_at", current_timestamp())
    .withColumn("_source", lit("products_csv"))
)

# Save as Delta table
(df_products_bronze
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.bronze_products")
)

print("bronze_products table created ✓")
display(df_products_bronze)

# COMMAND ----------

# This confirms all three Bronze tables exist
# and shows how many rows each one has

tables = ["bronze_orders", "bronze_customers", "bronze_products"]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as row_count FROM workspace.default.{table}").collect()[0][0]
    print(f"workspace.default.{table} → {count} rows ✓")

# COMMAND ----------

