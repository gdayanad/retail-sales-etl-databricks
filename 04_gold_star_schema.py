# Databricks notebook source
# ===========================================
# NOTEBOOK: 04_gold_star_schema
# PURPOSE:  Build star schema for dashboards
#           dim_date, dim_customer, dim_product
#           dim_channel, fact_orders
#           KPI view for BI consumption
# ===========================================

# import everything we need upfront
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    year,
    month,
    quarter,
    dayofmonth,
    date_format,
    monotonically_increasing_id, 
    round,
    sum,
    avg,
    count,
    countDistinct
)

# confirm all three silver tables are accessible
# before building Gold we always verify inputs exist
tables = ["silver_orders", "silver_customers", "silver_products"]

for table in tables:
    cnt = spark.sql(f"SELECT COUNT(*) FROM workspace.default.{table}").collect()[0][0]
    print(f"workspace.default.{table} → {cnt} rows ✓")

# COMMAND ----------

# ===========================================
# DIM_DATE — time dimension
# ===========================================

# dim_date is special — it's not built from our source data
# instead we generate a row for every single day in a date range
# this way we can always join any date to get year/month/quarter
# even for dates that have no orders yet
#
# sequence() generates a list of dates between two dates
# explode() turns that list into individual rows — one row per date
# we generate 3 years of dates — 2023 to 2025

from pyspark.sql.functions import explode, sequence, to_date

dim_date = spark.sql("""
    SELECT
        -- date_sk is the surrogate key for this dimension
        -- format 20240115 — easy to read and join on
        CAST(date_format(d, 'yyyyMMdd') AS INTEGER)  AS date_sk,

        d                                             AS full_date,

        -- extract individual date parts
        -- analysts use these to filter by year, month etc.
        YEAR(d)                                       AS year,
        QUARTER(d)                                    AS quarter,
        MONTH(d)                                      AS month,
        DAYOFMONTH(d)                                 AS day,

        -- day name — Monday, Tuesday etc.
        date_format(d, 'EEEE')                        AS day_name,

        -- month name — January, February etc.
        date_format(d, 'MMMM')                        AS month_name,

        -- week number in the year
        WEEKOFYEAR(d)                                 AS week_of_year,

        -- is this day a weekend?
        -- useful for retail analysis — weekends vs weekdays
        CASE
            WHEN date_format(d, 'EEEE') IN ('Saturday','Sunday')
            THEN 'Weekend'
            ELSE 'Weekday'
        END                                           AS day_type

    FROM (
        -- generate one row per day between these two dates
        SELECT explode(sequence(
            to_date('2023-01-01'),   -- start date
            to_date('2025-12-31'),   -- end date
            interval 1 day           -- one row per day
        )) AS d
    )
""")

# save as Gold Delta table
(dim_date
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.dim_date")
)

print(f"dim_date saved → {dim_date.count()} rows ✓")

# preview first few rows
display(spark.sql("""
    SELECT *
    FROM workspace.default.dim_date
    WHERE full_date BETWEEN '2024-01-15' AND '2024-01-18'
    ORDER BY full_date
"""))

# COMMAND ----------

# ===========================================
# DIM_CUSTOMER — customer dimension
# ===========================================

# dim_customer is built from silver_customers
# we add a surrogate key — customer_sk
# this is an artificial numeric key we create
# instead of using customer_id ("CUST01") directly
#
# why surrogate key?
# 1. integers join faster than strings
# 2. if customer_id changes in source system
#    our surrogate key stays stable
#    history is preserved
# 3. industry standard for dimension tables
#
# monotonically_increasing_id() generates a unique
# integer for each row automatically
# we don't control the exact numbers
# we just need them to be unique

df_silver_customers = spark.table("workspace.default.silver_customers")

dim_customer = (df_silver_customers
    # add surrogate key as first column
    .withColumn("customer_sk", monotonically_increasing_id())

    # select only columns needed for analysis
    # we don't need metadata columns like _ingest_at in Gold
    .select(
        "customer_sk",    # surrogate key — for joining
        "customer_id",    # natural key — original source ID
        "name",           # customer name
        "email",          # contact email
        "region",         # West, North, South, East
        "segment",        # Premium, Standard, Budget
        "country_code",   # DE, GB, IT, KR, SE
        "updated_at"      # when customer record last changed
    )
)

# save as Gold Delta table
(dim_customer
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.dim_customer")
)

print(f"dim_customer saved → {dim_customer.count()} rows ✓")

# preview
display(spark.sql("""
    SELECT *
    FROM workspace.default.dim_customer
    ORDER BY customer_sk
"""))

# COMMAND ----------

# ===========================================
# DIM_PRODUCT — product dimension
# ===========================================

# dim_product is built from silver_products
# same pattern as dim_customer:
# 1. read from silver
# 2. add surrogate key
# 3. select needed columns
# 4. save as Gold table

df_silver_products = spark.table("workspace.default.silver_products")

dim_product = (df_silver_products
    # add surrogate key
    .withColumn("product_sk", monotonically_increasing_id())

    .select(
        "product_sk",   # surrogate key — for joining to fact
        "product_id",   # natural key — original source ID
        "name",         # product name
        "category",     # Footwear, Accessories, Equipment, Apparel
        "brand",        # Nike, Adidas, Lululemon etc.
        "unit_cost"     # what WE paid — used for margin calculation
    )
)

# save as Gold Delta table
(dim_product
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.dim_product")
)

print(f"dim_product saved → {dim_product.count()} rows ✓")

display(spark.sql("""
    SELECT *
    FROM workspace.default.dim_product
    ORDER BY product_sk
"""))

# COMMAND ----------

# ===========================================
# DIM_CHANNEL — sales channel dimension
# ===========================================

# dim_channel is different from the other dimensions
# we don't read it from a silver table
# because channel is just a column in orders — not a separate source
#
# instead we extract the unique channel values
# directly from silver_orders
# this is called "deriving a dimension from a fact"
#
# our channels are: web, store, app
# in a real company you might also have:
# marketplace, wholesale, partner etc.

df_silver_orders = spark.table("workspace.default.silver_orders")

dim_channel = (df_silver_orders

    # select only the channel column
    .select("channel")

    # distinct() keeps only unique values
    # removes duplicates — like SELECT DISTINCT in SQL
    # we don't want web, web, web, store, store, app
    # we want web, store, app — one row per channel
    .distinct()

    # add surrogate key
    .withColumn("channel_sk", monotonically_increasing_id())

    # reorder columns — surrogate key first
    .select(
        "channel_sk",  # surrogate key
        "channel"      # channel name
    )
)

# save as Gold Delta table
(dim_channel
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.dim_channel")
)

print(f"dim_channel saved → {dim_channel.count()} rows ✓")

display(spark.sql("""
    SELECT *
    FROM workspace.default.dim_channel
    ORDER BY channel_sk
"""))

# COMMAND ----------

# ===========================================
# FACT_ORDERS — central fact table
# ===========================================

# fact_orders is the heart of the star schema
# it connects to every dimension via surrogate keys
# one row per order — that is our GRAIN
#
# GRAIN means: what does one row represent?
# our answer: one row = one order
#
# fact_orders contains:
# 1. surrogate keys linking to all dimensions
# 2. measurable values (amount, quantity, discount)
# 3. no descriptive text — that lives in dimensions
#
# we build it by joining silver_orders
# to each dimension on the natural key
# and keeping only the surrogate key from each dimension

# read silver_orders — our base for the fact table
df_orders = spark.table("workspace.default.silver_orders")

# read all dimensions
df_dim_customer = spark.table("workspace.default.dim_customer")
df_dim_product  = spark.table("workspace.default.dim_product")
df_dim_channel  = spark.table("workspace.default.dim_channel")
df_dim_date     = spark.table("workspace.default.dim_date")

# Step 1 — add date_sk to orders
# we need to convert order_date to the same integer format
# as dim_date.date_sk so we can join them
# format 20240115 = yyyyMMdd as integer
from pyspark.sql.functions import date_format

df_orders_with_datesk = (df_orders
    .withColumn(
        "date_sk",
        # convert order_date to integer format yyyyMMdd
        # same format as dim_date.date_sk
        col("order_date").cast("string")
        .substr(1, 10)              # keep only YYYY-MM-DD part
    )
)

# simpler way to get date_sk
df_orders_with_datesk = (df_orders
    .withColumn(
        "date_sk",
        date_format(col("order_date"), "yyyyMMdd").cast("integer")
    )
)

print(f"Orders with date_sk: {df_orders_with_datesk.count()} rows")
display(df_orders_with_datesk.select("order_id", "order_date", "date_sk"))

# COMMAND ----------

# ===========================================
# FACT_ORDERS — join all dimensions
# ===========================================

# we join silver_orders to each dimension
# using the natural key (customer_id, product_id etc.)
# and keep only the surrogate key from each dimension
#
# LEFT JOIN means:
# keep all orders even if no matching dimension row found
# better than INNER JOIN which would silently drop orders
# if a customer or product is missing from dimensions

fact_orders = (df_orders_with_datesk

    # join to dim_customer on customer_id
    # .alias() gives the DataFrame a short name for the join
    # so we can write "c.customer_sk" instead of
    # "df_dim_customer.customer_sk"
    .join(
        df_dim_customer.alias("c"),
        on="customer_id",
        how="left"
    )

    # join to dim_product on product_id
    .join(
        df_dim_product.alias("p"),
        on="product_id",
        how="left"
    )

    # join to dim_channel on channel
    .join(
        df_dim_channel.alias("ch"),
        on="channel",
        how="left"
    )

    # join to dim_date on date_sk
    .join(
        df_dim_date.alias("d"),
        on="date_sk",
        how="left"
    )

    # select only the columns we want in fact_orders
    # surrogate keys from dimensions + measures from orders
    .select(
        # natural key — keep for traceability
        "order_id",

        # surrogate keys — links to dimensions
        col("c.customer_sk"),    # links to dim_customer
        col("p.product_sk"),     # links to dim_product
        col("ch.channel_sk"),    # links to dim_channel
        "date_sk",               # links to dim_date

        # keep natural keys too — useful for debugging
        "customer_id",
        "product_id",
        "channel",
        "order_date",

        # measures — the numbers we analyse
        col("quantity"),         # how many units
        col("unit_price"),       # price per unit
        col("amount"),           # total order value
        col("discount_pct"),     # discount applied

        # calculated measure — profit margin
        # margin = amount - (quantity * unit_cost)
        # unit_cost comes from dim_product
        (col("amount") - (col("quantity") * col("p.unit_cost")))
        .alias("gross_profit"),

        # metadata
        col("_silver_at")
    )
)

print(f"fact_orders row count: {fact_orders.count()}")
display(fact_orders)

# COMMAND ----------

# ===========================================
# SAVE FACT_ORDERS
# ===========================================

# save fact_orders as a Gold Delta table
# partitionBy("order_date") means:
# data is physically split into separate folders by date
# e.g. order_date=2024-01-15/part-0001.parquet
#      order_date=2024-01-16/part-0001.parquet
#
# why partition by date?
# when a query filters by date:
# WHERE order_date = '2024-01-15'
# Spark only reads that one folder
# instead of scanning the entire table
# massively faster on large datasets
#
# for our 6 rows it makes no difference
# but it's the correct production pattern

(fact_orders
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("order_date")
    .saveAsTable("workspace.default.fact_orders")
)

print("fact_orders saved ✓")

# verify from disk
display(spark.sql("""
    SELECT
        order_id,
        customer_sk,
        product_sk,
        channel_sk,
        date_sk,
        quantity,
        amount,
        gross_profit,
        order_date
    FROM workspace.default.fact_orders
    ORDER BY order_date, order_id
"""))

# COMMAND ----------

# ===========================================
# KPI VIEW — for BI and dashboards
# ===========================================

# a VIEW is like a saved SQL query
# it doesn't store data — it runs the query
# every time someone reads it
# analysts query this view — they never touch
# the raw fact and dimension tables directly
#
# this view joins all tables and pre-calculates
# the KPIs management cares about:
# revenue, orders, units sold, avg order value
# gross profit, profit margin %

spark.sql("""
    CREATE OR REPLACE VIEW workspace.default.vw_sales_kpis AS
    SELECT
        -- time dimensions
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.day_type,          -- Weekday vs Weekend

        -- product dimensions
        p.category,
        p.brand,

        -- customer dimensions
        c.region,
        c.segment,
        c.country_code,

        -- channel dimension
        ch.channel,

        -- measures
        COUNT(DISTINCT f.order_id)        AS order_count,
        SUM(f.quantity)                   AS units_sold,
        ROUND(SUM(f.amount), 2)           AS revenue,
        ROUND(AVG(f.amount), 2)           AS avg_order_value,
        ROUND(SUM(f.gross_profit), 2)     AS gross_profit,

        -- profit margin % = gross profit / revenue * 100
        ROUND(SUM(f.gross_profit) /
              NULLIF(SUM(f.amount), 0) * 100, 1)
                                          AS margin_pct

    FROM workspace.default.fact_orders f

    -- join all dimensions
    JOIN workspace.default.dim_date     d  ON f.date_sk     = d.date_sk
    JOIN workspace.default.dim_product  p  ON f.product_sk  = p.product_sk
    JOIN workspace.default.dim_customer c  ON f.customer_sk = c.customer_sk
    JOIN workspace.default.dim_channel  ch ON f.channel_sk  = ch.channel_sk

    GROUP BY
        d.year, d.quarter, d.month, d.month_name, d.day_type,
        p.category, p.brand,
        c.region, c.segment, c.country_code,
        ch.channel
""")

print("vw_sales_kpis view created ✓")

# query the view — this is what a BI tool would run
display(spark.sql("""
    SELECT *
    FROM workspace.default.vw_sales_kpis
    ORDER BY revenue DESC
"""))

# COMMAND ----------

# ===========================================
# PHASE 4 SUMMARY CHECK
# ===========================================

gold_tables = [
    "dim_date",
    "dim_customer", 
    "dim_product",
    "dim_channel",
    "fact_orders"
]

print("=== GOLD TABLES ===")
for table in gold_tables:
    cnt = spark.sql(f"SELECT COUNT(*) FROM workspace.default.{table}").collect()[0][0]
    print(f"workspace.default.{table:20} → {cnt} rows ✓")

print("\n=== KPI VIEW ===")
kpi_cnt = spark.sql("SELECT COUNT(*) FROM workspace.default.vw_sales_kpis").collect()[0][0]
print(f"workspace.default.vw_sales_kpis → {kpi_cnt} rows ✓")