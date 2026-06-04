# Databricks notebook source
# ===========================================
# NOTEBOOK: 05_data_quality
# PURPOSE:  Run data quality tests across
#           all layers of the pipeline
#           Any failure stops the pipeline
#           and prints a clear error message
# ===========================================

# we build a simple test framework from scratch
# no external libraries needed
# just Python functions and PySpark

# Cell 1 ✅  defined the framework (run_test function)
#            test_results = []        ← still empty
#            run_test defined         ← ready to use

# Cell 2 ⏳  actually CALL run_test with real checks
# Cell 3 ⏳  more tests
# Cell 4 ⏳  print final summary

from pyspark.sql.functions import col, count, sum, round

# this list will collect results of every test
# at the end we print a summary of all tests
test_results = []

def run_test(test_name, passed, expected, actual, details=""):
    """
    Records result of one test.
    
    test_name → what are we testing
    passed    → True if test passed, False if failed
    expected  → what we expected to see
    actual    → what we actually saw
    details   → any extra information
    """
    status = "✅ PASS" if passed else "❌ FAIL"
    test_results.append({
        "test"    : test_name,
        "status"  : status,
        "expected": str(expected),
        "actual"  : str(actual),
        "details" : details
    })
    # print immediately so we see results as they run
    print(f"{status} | {test_name}")
    if not passed:
        print(f"         expected : {expected}")
        print(f"         actual   : {actual}")
        if details:
            print(f"         details  : {details}")

print("Test framework ready ✓")
print("Starting data quality checks...\n")

# COMMAND ----------

# First 3 Tests
# ===========================================
# TEST 1 — Row count check
# ===========================================

# we expect exactly 6 rows in silver_orders
# because we started with 9 bronze rows and:
# removed 2 bad rows (ORD006, ORD007)
# removed 1 duplicate (ORD001)
# 9 - 2 - 1 = 6

actual_count = spark.table("workspace.default.silver_orders").count()
expected_count = 6

run_test(
    test_name = "silver_orders row count",
    passed    = (actual_count == expected_count),
    expected  = expected_count,
    actual    = actual_count,
    details   = "expected 9 - 2 bad - 1 duplicate = 6"
)

# ===========================================
# TEST 2 — Null check on critical columns
# ===========================================

# in silver_orders these columns must NEVER be null
# order_id   → we can't identify the order without it
# product_id → we can't link to product dimension
# amount     → we can't calculate revenue without it
# order_date → we can't place it in time without it

critical_columns = ["order_id", "product_id", "amount", "order_date"]

df_silver = spark.table("workspace.default.silver_orders")

for column in critical_columns:
    # count rows where this column IS null
    null_count = (df_silver
        .filter(col(column).isNull())
        .count()
    )
    run_test(
        test_name = f"no nulls in silver_orders.{column}",
        passed    = (null_count == 0),
        expected  = 0,
        actual    = null_count,
        details   = f"found {null_count} null values in {column}"
    )

# ===========================================
# TEST 3 — Uniqueness check
# ===========================================

# order_id must be unique in silver_orders
# one order_id = one order, always
# if we find duplicates, our dedup logic failed

total_rows  = df_silver.count()
unique_rows = df_silver.select("order_id").distinct().count()

run_test(
    test_name = "order_id is unique in silver_orders",
    passed    = (total_rows == unique_rows),
    expected  = f"{total_rows} unique rows",
    actual    = f"{unique_rows} unique rows",
    details   = f"found {total_rows - unique_rows} duplicate(s)"
)

# COMMAND ----------

# explicitly import Python's built-in round
# to avoid any conflict with PySpark's round function
import builtins

# ===========================================
# TEST 4 — No negative amounts in silver
# ===========================================

df_silver = spark.table("workspace.default.silver_orders")

negative_count = (df_silver
    .filter(col("amount") < 0)
    .count()
)

run_test(
    test_name = "no negative amounts in silver_orders",
    passed    = (negative_count == 0),
    expected  = 0,
    actual    = negative_count,
    details   = f"found {negative_count} negative amounts"
)

# ===========================================
# TEST 5 — Referential integrity
#           every order has a valid customer
# ===========================================

df_customers = spark.table("workspace.default.silver_customers")

orphan_orders = (df_silver
    .join(df_customers, on="customer_id", how="left")
    .filter(col("name").isNull())
    .count()
)

run_test(
    test_name = "all orders have valid customer_id",
    passed    = (orphan_orders == 0),
    expected  = 0,
    actual    = orphan_orders,
    details   = f"found {orphan_orders} orders with no matching customer"
)

# ===========================================
# TEST 6 — Referential integrity
#           every order has a valid product
# ===========================================

df_products = spark.table("workspace.default.silver_products")

orphan_products = (df_silver
    .join(df_products, on="product_id", how="left")
    .filter(col("name").isNull())
    .count()
)

run_test(
    test_name = "all orders have valid product_id",
    passed    = (orphan_products == 0),
    expected  = 0,
    actual    = orphan_products,
    details   = f"found {orphan_products} orders with no matching product"
)

# ===========================================
# TEST 7 — Revenue reconciliation
# ===========================================

from pyspark.sql.functions import col, sum as spark_sum

bronze_revenue = (spark
    .table("workspace.default.bronze_orders")
    .filter(col("amount") > 0)
    .filter(col("customer_id").isNotNull())
    .agg(spark_sum("amount"))
    .collect()[0][0]
)

silver_revenue = (spark
    .table("workspace.default.silver_orders")
    .agg(spark_sum("amount"))
    .collect()[0][0]
)

# use builtins.round to avoid conflict with PySpark round
bronze_revenue = builtins.round(bronze_revenue, 2)
silver_revenue = builtins.round(silver_revenue, 2)

run_test(
    test_name = "revenue reconciliation bronze vs silver",
    passed    = (bronze_revenue == silver_revenue),
    expected  = bronze_revenue,
    actual    = silver_revenue,
    details   = f"bronze={bronze_revenue}, silver={silver_revenue}"
)

# COMMAND ----------

# ===========================================
# INVESTIGATE revenue reconciliation failure
# ===========================================

# let's see exactly which rows are in bronze
# that we're counting as "valid"
print("=== BRONZE valid rows (amount > 0 AND customer_id not null) ===")
display(spark.sql("""
    SELECT order_id, customer_id, amount
    FROM workspace.default.bronze_orders
    WHERE amount > 0
    AND customer_id IS NOT NULL
    ORDER BY order_id
"""))

print("=== SILVER rows ===")
display(spark.sql("""
    SELECT order_id, customer_id, amount
    FROM workspace.default.silver_orders
    ORDER BY order_id
"""))

print("=== BRONZE valid total ===")
display(spark.sql("""
    SELECT ROUND(SUM(amount), 2) as bronze_total
    FROM workspace.default.bronze_orders
    WHERE amount > 0
    AND customer_id IS NOT NULL
"""))

print("=== SILVER total ===")
display(spark.sql("""
    SELECT ROUND(SUM(amount), 2) as silver_total
    FROM workspace.default.silver_orders
"""))

# COMMAND ----------

# ===========================================
# TEST 7 — Revenue reconciliation (fixed)
# ===========================================
#exclude duplicates from bronze when reconciling
# bronze has duplicates — we must deduplicate
# before comparing revenue to silver
# otherwise bronze total will always be higher
# by the amount of duplicated orders

bronze_revenue = (spark
    .table("workspace.default.bronze_orders")
    .filter(col("amount") > 0)              # exclude negative amounts
    .filter(col("customer_id").isNotNull()) # exclude missing customers
    .dropDuplicates(["order_id"])           # exclude duplicates ← fix
    .agg(spark_sum("amount"))
    .collect()[0][0]
)

silver_revenue = (spark
    .table("workspace.default.silver_orders")
    .agg(spark_sum("amount"))
    .collect()[0][0]
)

bronze_revenue = builtins.round(bronze_revenue, 2)
silver_revenue = builtins.round(silver_revenue, 2)

run_test(
    test_name = "revenue reconciliation bronze vs silver",
    passed    = (bronze_revenue == silver_revenue),
    expected  = bronze_revenue,
    actual    = silver_revenue,
    details   = f"bronze={bronze_revenue}, silver={silver_revenue}"
)

# COMMAND ----------

# ===========================================
# TEST 8 — Freshness check
# ===========================================

# data freshness means: is our data recent enough?
# in production this would check:
# "was this table updated in the last 24 hours?"
# for our project we check if silver_orders
# has data from 2024 — our expected date range

from pyspark.sql.functions import max as spark_max

latest_date = (spark
    .table("workspace.default.silver_orders")
    .agg(spark_max("order_date"))
    .collect()[0][0]
)

# confirm latest date is within our expected range
# in production you'd compare to current_date()
expected_latest = "2024-01-18"

run_test(
    test_name = "silver_orders freshness check",
    passed    = (str(latest_date) == expected_latest),
    expected  = expected_latest,
    actual    = str(latest_date),
    details   = f"latest order date is {latest_date}"
)

# ===========================================
# TEST 9 — Gross profit sanity check
# ===========================================

# gross profit should always be positive
# gross_profit = amount - (quantity * unit_cost)
# if negative — we sold below cost price
# that's either a data error or a serious business problem
# for our data all products should have positive margin

negative_profit = (spark
    .table("workspace.default.fact_orders")
    .filter(col("gross_profit") < 0)
    .count()
)

run_test(
    test_name = "no negative gross profit in fact_orders",
    passed    = (negative_profit == 0),
    expected  = 0,
    actual    = negative_profit,
    details   = f"found {negative_profit} orders with negative profit"
)

# ===========================================
# TEST 10 — Schema check
# ===========================================

# confirm all expected columns exist in silver_orders
# if someone accidentally drops a column upstream
# this test catches it immediately
# before it breaks downstream Gold tables

expected_columns = [
    "order_id",
    "customer_id",
    "product_id",
    "order_date",
    "quantity",
    "unit_price",
    "amount",
    "channel",
    "discount_pct"
]

actual_columns = spark.table("workspace.default.silver_orders").columns

# check every expected column exists
missing_columns = [c for c in expected_columns if c not in actual_columns]

run_test(
    test_name = "silver_orders schema check",
    passed    = (len(missing_columns) == 0),
    expected  = "all columns present",
    actual    = f"{len(missing_columns)} missing",
    details   = f"missing: {missing_columns}" if missing_columns else "all columns found"
)

# COMMAND ----------

# ===========================================
# FINAL SUMMARY — all test results
# ===========================================

# convert test_results list into a DataFrame
# so we can display it as a nice table
#
# test_results is a list of dictionaries
# spark.createDataFrame() can convert that directly
# into a DataFrame with one row per test

df_results = spark.createDataFrame(test_results)

# count passes and failures
total    = df_results.count()
passed   = df_results.filter(col("status") == "✅ PASS").count()
failed   = df_results.filter(col("status") == "❌ FAIL").count()

print("=" * 50)
print(f"  DATA QUALITY SUMMARY")
print("=" * 50)
print(f"  Total tests  : {total}")
print(f"  Passed       : {passed}")
print(f"  Failed       : {failed}")
print("=" * 50)

if failed == 0:
    print("  ✅ ALL TESTS PASSED — pipeline is healthy")
else:
    print(f"  ❌ {failed} TEST(S) FAILED — investigate before proceeding")
print("=" * 50)

# display full results table
display(df_results.select(
    "status",
    "test",
    "expected",
    "actual",
    "details"
).orderBy("status"))