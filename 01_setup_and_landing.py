# Databricks notebook source
# MAGIC %md
# MAGIC Library
# MAGIC ├── Section: Science
# MAGIC │   ├── Shelf: Physics
# MAGIC │   │   ├── Book 1
# MAGIC │   │   ├── Book 2
# MAGIC │   └── Shelf: Chemistry
# MAGIC │       ├── Book 1
# MAGIC └── Section: History
# MAGIC     ├── Shelf: Ancient
# MAGIC     │   ├── Book 1
# MAGIC
# MAGIC In Databricks the same structure exists:
# MAGIC Databricks Workspace
# MAGIC ├── Catalog: workspace
# MAGIC │   ├── Schema: default
# MAGIC │   │   ├── Table: bronze_orders
# MAGIC │   │   ├── Table: bronze_customers
# MAGIC │   │   └── Volume: retail_data
# MAGIC │   └── Schema: information_schema
# MAGIC ├── Catalog: samples
# MAGIC └── Catalog: system
# MAGIC
# MAGIC So what is a container?
# MAGIC A container is simply something that holds other things.
# MAGIC Catalog   → contains schemas
# MAGIC Schema    → contains tables and volumes
# MAGIC Volume    → contains files
# MAGIC Table     → contains rows and columns
# MAGIC
# MAGIC Each level wraps the level below it:
# MAGIC Catalog
# MAGIC └── contains Schemas
# MAGIC         └── contains Tables and Volumes
# MAGIC                     └── contains data
# MAGIC
# MAGIC workspace          ← catalog (container for schemas)
# MAGIC └── default        ← schema (container for tables + volumes)
# MAGIC     ├── bronze_orders      ← table (container for rows)
# MAGIC     ├── bronze_customers   ← table
# MAGIC     ├── bronze_products    ← table
# MAGIC     └── retail_data        ← volume (container for files)
# MAGIC         └── landing/
# MAGIC             └── orders.csv

# COMMAND ----------

# spark.sql() runs a SQL command inside python
# SHOW CATALOGS lists all the containers in your DATABRICKS workspace
# display() shows results in a nice table
# below codes gives us what storages are avaible to us

display(spark.sql("SHOW CATALOGS"))

# COMMAND ----------

# A schema is a named container inside a catalog that groups related tables and volumes together.
# workspace (catalog)
# └── default (schema)     ← the only drawer you have
#     ├── bronze_orders
#     ├── bronze_customers
#     └── bronze_products

display(spark.sql("SHOW SCHEMAS IN workspace"))


#  In a real company setup
# A real data team would create separate schemas for each layer:
# python# create separate schemas for each layer
# spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
# spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
# spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

# spark.sql("SHOW SCHEMAS IN workspace")    # newer syntax
# spark.sql("SHOW DATABASES IN workspace")  # older syntax — same result

# COMMAND ----------

# volumes is a cloud storage that lives inside Databricks catalog
display(spark.sql("SHOW VOLUMES IN workspace.default"))

# COMMAND ----------

# This creates a Volume called "retail_data" inside workspace.default
# A Volume is a managed storage location in Unity Catalog
# Think of it as: workspace (catalog) → default (schema) → retail_data (volume)

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.retail_data")

# Confirm it was created
display(spark.sql("SHOW VOLUMES IN workspace.default"))

# COMMAND ----------

# Now create the folder structure inside the Volume
# The path pattern for Volumes is always:
# /Volumes/catalog/schema/volume_name/your/folders/here

dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/landing/orders")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/landing/customers")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/landing/products")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/bronze")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/silver")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/gold")
dbutils.fs.mkdirs("/Volumes/workspace/default/retail_data/quarantine")

# Confirm all folders exist
display(dbutils.fs.ls("/Volumes/workspace/default/retail_data/"))

# COMMAND ----------


# The open() / f.write() block saves that string to a file:
# open(..., "w") opens (or creates) a file at that path in write mode
# as f gives you a file object named f to work with
# f.write(orders_data) writes the string into the file
# with ensures the file is properly closed afterward, even if an error occurs

orders_data = """order_id,customer_id,product_id,order_date,quantity,unit_price,amount,channel,discount_pct
ORD001,CUST01,PROD01,2024-01-15,2,29.99,59.98,web,0
ORD002,CUST02,PROD03,2024-01-15,1,149.99,149.99,store,5
ORD003,CUST01,PROD02,2024-01-16,3,9.99,29.97,web,0
ORD004,CUST03,PROD01,2024-01-16,1,29.99,29.99,app,10
ORD005,CUST04,PROD04,2024-01-17,2,79.99,159.98,web,0
ORD006,,PROD02,2024-01-17,1,9.99,9.99,web,0
ORD007,CUST02,PROD03,2024-01-18,1,149.99,-149.99,store,0
ORD008,CUST05,PROD05,2024-01-18,4,19.99,79.96,app,0
ORD001,CUST01,PROD01,2024-01-15,2,29.99,59.98,web,0"""

with open("/Volumes/workspace/default/retail_data/landing/orders/orders.csv", "w") as f:
    f.write(orders_data)

print("orders.csv written ✓")


# COMMAND ----------


customers_data = """[
  {"customer_id":"CUST01","name":"Anna Müller","email":"anna@example.com","region":"West","segment":"Premium","country_code":"DE","updated_at":"2024-01-10"},
  {"customer_id":"CUST02","name":"Ben Smith","email":"ben@example.com","region":"North","segment":"Standard","country_code":"GB","updated_at":"2024-01-08"},
  {"customer_id":"CUST03","name":"Clara Rossi","email":"clara@example.com","region":"South","segment":"Premium","country_code":"IT","updated_at":"2024-01-12"},
  {"customer_id":"CUST04","name":"David Park","email":"david@example.com","region":"East","segment":"Standard","country_code":"KR","updated_at":"2024-01-09"},
  {"customer_id":"CUST05","name":"Eva Johansson","email":"eva@example.com","region":"North","segment":"Budget","country_code":"SE","updated_at":"2024-01-11"}
]"""

with open("/Volumes/workspace/default/retail_data/landing/customers/customers.json", "w") as f:
    f.write(customers_data)

print("customers.json written ✓")

# COMMAND ----------

products_data = """product_id,name,category,brand,unit_cost
PROD01,Running Shoes,Footwear,Nike,18.00
PROD02,Water Bottle,Accessories,Generic,3.50
PROD03,Yoga Mat,Equipment,Lululemon,60.00
PROD04,Resistance Band,Equipment,TheraBand,25.00
PROD05,Sports Socks,Apparel,Adidas,8.00"""

with open("/Volumes/workspace/default/retail_data/landing/products/products.csv", "w") as f:
    f.write(products_data)

print("products.csv written ✓")

# COMMAND ----------

# Check all three files exist in their folders
# fs.ls() is essentially a "list directory" 

print("=== ORDERS ===")
display(dbutils.fs.ls("/Volumes/workspace/default/retail_data/landing/orders/"))

print("=== CUSTOMERS ===")
display(dbutils.fs.ls("/Volumes/workspace/default/retail_data/landing/customers/"))

print("=== PRODUCTS ===")
display(dbutils.fs.ls("/Volumes/workspace/default/retail_data/landing/products/"))