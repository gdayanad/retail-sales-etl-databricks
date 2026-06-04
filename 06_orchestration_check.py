# Databricks notebook source
# ===========================================
# NOTEBOOK: 06_orchestration_check
# PURPOSE:  Final pipeline health check
#           Run after all other notebooks
#           Confirms entire pipeline ran correctly
#           Reports final status
# ===========================================

print("=" * 55)
print("  RETAIL SALES ETL — PIPELINE RUN SUMMARY")
print("=" * 55)

# check all tables exist and have correct row counts
checks = {
    "bronze_orders"    : 9,
    "bronze_customers" : 5,
    "bronze_products"  : 5,
    "silver_orders"    : 6,
    "silver_customers" : 5,
    "silver_products"  : 5,
    "quarantine_orders": 2,
    "dim_date"         : 1096,
    "dim_customer"     : 5,
    "dim_product"      : 5,
    "dim_channel"      : 3,
    "fact_orders"      : 6,
}

all_passed = True

for table, expected_count in checks.items():
    try:
        # try to read the table and count rows
        actual_count = spark.sql(
            f"SELECT COUNT(*) FROM workspace.default.{table}"
        ).collect()[0][0]

        passed = (actual_count == expected_count)

        if not passed:
            all_passed = False

        status = "✅" if passed else "❌"
        print(f"  {status} {table:30} → {actual_count} rows")

    except Exception as e:
        # if table doesn't exist at all
        all_passed = False
        print(f"  ❌ {table:30} → TABLE NOT FOUND")

print("=" * 55)

# check KPI view works
try:
    kpi_count = spark.sql(
        "SELECT COUNT(*) FROM workspace.default.vw_sales_kpis"
    ).collect()[0][0]
    print(f"  ✅ vw_sales_kpis view          → {kpi_count} rows")
except Exception as e:
    all_passed = False
    print(f"  ❌ vw_sales_kpis view          → VIEW NOT FOUND")

print("=" * 55)

if all_passed:
    print("  ✅ PIPELINE COMPLETE — all checks passed")
    print("  ✅ Data is ready for dashboards")
else:
    print("  ❌ PIPELINE FAILED — investigate errors above")

print("=" * 55)