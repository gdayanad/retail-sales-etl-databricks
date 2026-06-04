# Databricks notebook source
#  Bronze is raw data problem included. bronze_orders (9 rows)
#├── ORD006 — missing customer_id    ← bad row
#── ORD007 — negative amount        ← bad row
#└── ORD001 — duplicate row          ← bad row

# Silver will
# Step 1 → read bronze_orders
# Step 2 → label every bad row with a reason
# Step 3 → send bad rows to quarantine table
# Step 4 → deduplicate good rows
# Step 5 → fix data types properly
# Step 6 → save clean data as silver_orders


# COMMAND ----------

# ===========================================
# NOTEBOOK: 03_silver_transform
# PURPOSE:  Clean and validate Bronze data
#           Bad rows  → quarantine table
#           Good rows → Silver Delta tables
# ===========================================

# import all functions we need upfront
# good habit — always import at the top of the notebook
from pyspark.sql.functions import (
    col,               # reference a column by name
    lit,               # fixed value same for every row
    current_timestamp, # timestamp of right now
    when,              # if/else logic
    trim,              # remove spaces from text
    upper,             # convert text to uppercase
    to_date            # convert text to a date type
)

# read bronze_orders — our starting point for Silver
# we always read from Bronze, never from the raw CSV
# Bronze is our source of truth for what arrived
df_bronze = spark.table("workspace.default.bronze_orders")

# quick sanity check — how many rows came in?
print(f"Bronze row count: {df_bronze.count()}")
display(df_bronze)

# COMMAND ----------

# ===========================================
# STEP 2 — Label bad rows
# ===========================================

# we add a new column called "data_issue"
# every row gets a label explaining what is wrong with it
# good rows get the label "ok"
# bad rows get a specific reason
#
# ORDER MATTERS — most specific condition first
# if a row has BOTH problems, we catch it at the top
# if we put single checks first, we'd never reach the combined check

df_labeled = (df_bronze
    .withColumn("data_issue",
        when(
            # condition 1 — BOTH customer_id missing AND amount negative
            # most specific — must be checked first
            col("customer_id").isNull() & (col("amount") < 0),
            "missing customer and negative amount"
        )
        .when(
            # condition 2 — customer_id is empty
            col("customer_id").isNull(),
            "missing customer_id"
        )
        .when(
            # condition 3 — amount is negative (refund or data error)
            col("amount") < 0,
            "negative amount"
        )
        .otherwise("ok")   # no issues found — good row
    )
)

# let's see what label every row got
display(df_labeled.select("order_id", "customer_id", "amount", "data_issue"))

# COMMAND ----------

# ===========================================
# STEP 3 — Split good rows and bad rows
# ===========================================

# filter rows where data_issue = "ok" → these are good rows
# filter rows where data_issue != "ok" → these are bad rows
# != means "not equal to"

df_good = df_labeled.filter(col("data_issue") == "ok")
df_bad  = df_labeled.filter(col("data_issue") != "ok")

# how many in each group?
print(f"Good rows : {df_good.count()}")   # should be 7
print(f"Bad rows  : {df_bad.count()}")    # should be 2

print("\n=== BAD ROWS ===")
display(df_bad.select("order_id", "customer_id", "amount", "data_issue"))

print("\n=== GOOD ROWS ===")
display(df_good.select("order_id", "customer_id", "amount", "data_issue"))

# COMMAND ----------

# ===========================================
# STEP 4 — Save bad rows to quarantine
# ===========================================

# bad rows never get silently deleted
# they go to a quarantine table so we can:
# 1. investigate what went wrong
# 2. fix the source data if needed
# 3. reprocess them later
# 4. prove to auditors nothing was lost

# we add one more column — _quarantine_at
# this records exactly when the row was quarantined
# useful for tracking how long bad data sits unresolved

df_quarantine = (df_bad
    .withColumn("_quarantine_at", current_timestamp())
)

# save to quarantine table
# mode("append") means add to existing rows
# if we run this pipeline tomorrow, new bad rows
# are added, not overwriting today's bad rows
(df_quarantine
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("workspace.default.quarantine_orders")
)

print(f"Quarantined {df_quarantine.count()} rows ✓")

# let's see what's in the quarantine table
display(spark.table("workspace.default.quarantine_orders"))

# COMMAND ----------

# Why append mode here?
# bronze_orders   → overwrite  (reload fresh every time)
# quarantine      → append     (accumulate bad rows over time)
# silver_orders   → overwrite  (always latest clean data)
# Quarantine uses append because you want a growing history of every bad row that ever came through — not just today's. Tell me what you see!

# COMMAND ----------

# ===========================================
# STEP 5 — Remove duplicates
# ===========================================

# remember ORD001 appears twice in bronze
# both rows are labeled "ok" because duplicate
# is not a data_issue label — it's a separate problem
#
# dropDuplicates(["order_id"]) means:
# "if two rows have the same order_id, keep only the first one"
# we use order_id as the business key — it must be unique
# one order_id = one order, always

df_deduped = df_good.dropDuplicates(["order_id"])

# confirm duplicate was removed
print(f"Before dedup : {df_good.count()} rows")
print(f"After dedup  : {df_deduped.count()} rows")
print(f"Removed      : {df_good.count() - df_deduped.count()} duplicate(s)")

display(df_deduped.select("order_id", "customer_id", "amount", "data_issue"))

# COMMAND ----------

# ===========================================
# STEP 6 — Clean and fix data types
# ===========================================

# even though inferSchema guessed most types correctly
# we always explicitly cast types in Silver
# reason: we never trust inferSchema in production
# explicit casting = we are in control, not Spark's guess
#
# we also clean text columns:
# trim() removes accidental spaces
# e.g. "web " → "web" (trailing space would break joins later)

df_cleaned = (df_deduped

    # fix text columns — remove accidental spaces
    .withColumn("order_id",     trim(col("order_id")))
    .withColumn("customer_id",  trim(col("customer_id")))
    .withColumn("product_id",   trim(col("product_id")))
    .withColumn("channel",      trim(col("channel")))

    # explicitly cast each column to correct type
    # cast() converts a column to the type you specify
    # even if inferSchema got it right, we make it explicit
    .withColumn("order_date",   col("order_date").cast("date"))
    .withColumn("quantity",     col("quantity").cast("integer"))
    .withColumn("unit_price",   col("unit_price").cast("double"))
    .withColumn("amount",       col("amount").cast("double"))
    .withColumn("discount_pct", col("discount_pct").cast("integer"))

    # drop the data_issue column — it was only needed for filtering
    # Silver tables should be clean, no helper columns
    .drop("data_issue")

    # add Silver metadata columns
    # _silver_at tells us when this row was cleaned and promoted
    .withColumn("_silver_at", current_timestamp())
)

# confirm schema looks correct
print("=== Silver Schema ===")
df_cleaned.printSchema()

print(f"\nClean row count: {df_cleaned.count()}")
display(df_cleaned)

# COMMAND ----------

# ===========================================
# STEP 7 — Save as Silver Delta table
# ===========================================

# we now save the clean, deduplicated, validated data
# as a Silver Delta table
#
# mode("overwrite") because:
# every time this pipeline runs, we rebuild Silver from Bronze
# Bronze is always the source of truth
# Silver is always a clean reflection of Bronze
#
# in production you would use MERGE instead of overwrite
# MERGE updates existing rows and inserts new ones
# we will cover MERGE in a later phase
# for now overwrite is correct for our small dataset

(df_cleaned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_orders")
)

print("silver_orders saved ✓")

# final check — query the silver table directly
# this confirms it was saved correctly in the catalog
display(spark.sql("""
    SELECT
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        amount,
        channel,
        discount_pct,
        _silver_at
    FROM workspace.default.silver_orders
    ORDER BY order_date, order_id
"""))

# COMMAND ----------

# ===========================================
# SILVER CUSTOMERS
# ===========================================

# read from bronze_customers
df_cust_bronze = spark.table("workspace.default.bronze_customers")

print(f"Bronze customers row count: {df_cust_bronze.count()}")
display(df_cust_bronze)

# COMMAND ----------

# ===========================================
# STEP 2 — Label bad rows in customers
# ===========================================

# customers data is cleaner than orders
# but we still check for:
# 1. missing customer_id    → can't identify the customer
# 2. missing name           → incomplete record
# 3. missing country_code   → needed for regional analysis in Gold

df_cust_labeled = (df_cust_bronze
    .withColumn("data_issue",
        when(col("customer_id").isNull(), "missing customer_id")
        .when(col("name").isNull(),       "missing name")
        .when(col("country_code").isNull(),"missing country_code")
        .otherwise("ok")
    )
)

display(df_cust_labeled.select("customer_id", "name", "country_code", "data_issue"))

# COMMAND ----------

# ===========================================
# STEP 3 — Split, clean and save silver_customers
# ===========================================

# split good and bad rows
df_cust_good = df_cust_labeled.filter(col("data_issue") == "ok")
df_cust_bad  = df_cust_labeled.filter(col("data_issue") != "ok")

print(f"Good customers : {df_cust_good.count()}")
print(f"Bad customers  : {df_cust_bad.count()}")

# save bad rows to quarantine if any exist
# if count is 0, nothing gets written — that's fine
if df_cust_bad.count() > 0:
    (df_cust_bad
        .withColumn("_quarantine_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.default.quarantine_customers")
    )
    print(f"Quarantined {df_cust_bad.count()} customer rows ✓")
else:
    print("No bad customer rows — quarantine skipped ✓")

# clean good rows
# trim() removes accidental spaces from text columns
# cast() makes sure types are exactly what we expect
df_cust_cleaned = (df_cust_good

    # clean text columns
    .withColumn("customer_id",   trim(col("customer_id")))
    .withColumn("name",          trim(col("name")))
    .withColumn("email",         trim(col("email")))
    .withColumn("region",        trim(col("region")))
    .withColumn("segment",       trim(col("segment")))

    # standardize country_code to uppercase
    # "de" and "DE" should be the same country
    # upper() makes sure all country codes are consistent
    .withColumn("country_code",  upper(trim(col("country_code"))))

    # cast updated_at to date type
    # it came in as a string "2024-01-10"
    # we cast it to a proper date for comparisons later
    .withColumn("updated_at",    col("updated_at").cast("date"))

    # drop the helper column — not needed in Silver
    .drop("data_issue")

    # add silver metadata timestamp
    .withColumn("_silver_at", current_timestamp())
)

# save as silver_customers
(df_cust_cleaned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_customers")
)

print("silver_customers saved ✓")

# verify from disk
display(spark.sql("""
    SELECT
        customer_id,
        name,
        email,
        region,
        segment,
        country_code,
        updated_at,
        _silver_at
    FROM workspace.default.silver_customers
    ORDER BY customer_id
"""))

# COMMAND ----------

# ===========================================
# SILVER PRODUCTS
# ===========================================

# read bronze_products
df_prod_bronze = spark.table("workspace.default.bronze_products")

print(f"Bronze products row count: {df_prod_bronze.count()}")

# label bad rows
# for products we check:
# 1. missing product_id  → can't identify the product
# 2. missing name        → incomplete record
# 3. missing category    → needed for Gold KPI by category
# 4. unit_cost is null   → needed for margin calculation in Gold

df_prod_labeled = (df_prod_bronze
    .withColumn("data_issue",
        when(col("product_id").isNull(),  "missing product_id")
        .when(col("name").isNull(),       "missing name")
        .when(col("category").isNull(),   "missing category")
        .when(col("unit_cost").isNull(),  "missing unit_cost")
        .otherwise("ok")
    )
)

# split good and bad
df_prod_good = df_prod_labeled.filter(col("data_issue") == "ok")
df_prod_bad  = df_prod_labeled.filter(col("data_issue") != "ok")

print(f"Good products : {df_prod_good.count()}")
print(f"Bad products  : {df_prod_bad.count()}")

# quarantine bad rows if any
if df_prod_bad.count() > 0:
    (df_prod_bad
        .withColumn("_quarantine_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable("workspace.default.quarantine_products")
    )
    print(f"Quarantined {df_prod_bad.count()} product rows ✓")
else:
    print("No bad product rows — quarantine skipped ✓")

# clean good rows
df_prod_cleaned = (df_prod_good

    # clean text columns
    .withColumn("product_id",  trim(col("product_id")))
    .withColumn("name",        trim(col("name")))
    .withColumn("category",    trim(col("category")))
    .withColumn("brand",       trim(col("brand")))

    # cast unit_cost to double
    # unit_cost is the price WE paid for the product
    # used later in Gold to calculate profit margin
    .withColumn("unit_cost",   col("unit_cost").cast("double"))

    # drop helper column
    .drop("data_issue")

    # add silver metadata
    .withColumn("_silver_at", current_timestamp())
)

# save as silver_products
(df_prod_cleaned
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_products")
)

print("silver_products saved ✓")

# verify from disk
display(spark.sql("""
    SELECT
        product_id,
        name,
        category,
        brand,
        unit_cost,
        _silver_at
    FROM workspace.default.silver_products
    ORDER BY product_id
"""))

# COMMAND ----------

# final Phase 3 summary
tables = ["silver_orders", "silver_customers", "silver_products"]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as count FROM workspace.default.{table}").collect()[0][0]
    print(f"workspace.default.{table} → {count} rows ✓")