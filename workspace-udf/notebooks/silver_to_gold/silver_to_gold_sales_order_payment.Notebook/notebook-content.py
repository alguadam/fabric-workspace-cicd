# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63081e19-3045-4aa1-b19f-6425719fe664",
# META       "default_lakehouse_name": "maag_gold",
# META       "default_lakehouse_workspace_id": "82249858-6617-405c-b4be-d40ea331378b",
# META       "known_lakehouses": [
# META         {
# META           "id": "63081e19-3045-4aa1-b19f-6425719fe664"
# META         },
# META         {
# META           "id": "400ee61d-0fae-4cb3-9d1d-e66011e11d3b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load Silver Table to Gold Table - OrderPayment
# 
# ## Overview
# Load OrderPayment data from Silver lakehouse table to Gold lakehouse table.
# 
# ## Data Flow
# - **Source**: MAAG_LH_Silver.sales.OrderPayment (Silver lakehouse table)
# - **Target**: MAAG_LH_Gold.sales.OrderPayment (Gold lakehouse - attached as default)
# - **Process**: Read Silver table, apply transformations, load to Gold Delta table

# CELL ********************

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum, current_timestamp
import os
import sempy.fabric as fabric

# Configuration - Silver to Gold data flow
# Get workspace ID dynamically at runtime (avoids issues with spaces in workspace names)
WORKSPACE_ID = fabric.get_notebook_workspace_id()

# Get lakehouse ID dynamically (avoids issues with lakehouse names)
lakehouse_properties = mssparkutils.lakehouse.get("MAAG_LH_Silver")
SOURCE_LAKEHOUSE_ID = lakehouse_properties.id

SOURCE_SCHEMA = "sales"
SOURCE_TABLE = "OrderPayment"

# Source: Absolute path to Silver lakehouse table
SOURCE_TABLE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_LAKEHOUSE_ID}/Tables/{SOURCE_SCHEMA}/{SOURCE_TABLE}"

# Target: Gold lakehouse (attached as default)
TARGET_SCHEMA = "sales"
TARGET_TABLE = "OrderPayment"
TARGET_FULL_PATH = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"🔄 Loading OrderPayment from Silver to Gold")
print(f"📂 Source: {SOURCE_TABLE_PATH}")
print(f"🎯 Target: {TARGET_FULL_PATH}")
print("="*50)

# Read from Silver lakehouse table
df = spark.read.format("delta").load(SOURCE_TABLE_PATH)

print(f"✅ Data loaded from Silver table")
print(f"📊 Records: {df.count()}")
print(f"📋 Columns: {df.columns}")

# Display sample data
print(f"\n📖 Sample data from Silver:")
df.show(10, truncate=False)

# CELL ********************

# --- Gold layer transformations and data quality ---
print(f"🔧 Applying Gold layer transformations...")

# Add audit columns for Gold layer
df_gold = df.withColumn("GoldLoadTimestamp", current_timestamp())

# Data quality checks for Gold layer
print(f"\n🔍 Gold layer data quality validation...")

# Check for duplicates
duplicate_count = df_gold.groupBy("OrderId", "TransactionId").count().filter(col("count") > 1).count()
if duplicate_count > 0:
    print(f"⚠️ Found {duplicate_count} duplicate OrderId+TransactionId combinations")
else:
    print(f"✅ No duplicates found")

# Check for nulls in key fields
null_checks = df_gold.select(
    spark_sum(col("OrderId").isNull().cast("int")).alias("null_orderid"),
    spark_sum(col("TransactionId").isNull().cast("int")).alias("null_transactionid"),
    spark_sum(col("PaymentMethod").isNull().cast("int")).alias("null_paymentmethod")
).collect()[0]

if null_checks["null_orderid"] > 0 or null_checks["null_transactionid"] > 0 or null_checks["null_paymentmethod"] > 0:
    print(f"⚠️ Found nulls: OrderId={null_checks['null_orderid']}, TransactionId={null_checks['null_transactionid']}, PaymentMethod={null_checks['null_paymentmethod']}")
else:
    print(f"✅ No nulls in key fields")

print(f"\n📖 Sample Gold data:")
df_gold.show(10, truncate=False)

# CELL ********************

# --- Load data to Gold table ---
print(f"💾 Loading data to Gold table: {TARGET_FULL_PATH}")

try:
    # Write to Gold Delta table (default lakehouse)
    df_gold.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(TARGET_FULL_PATH)

    print(f"✅ Data loaded successfully to Gold table")

    # Verify the load
    result_count = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_FULL_PATH}").collect()[0]["count"]
    print(f"📊 Records in Gold table: {result_count}")

    # Show sample of loaded Gold data
    print(f"\n📖 Sample from Gold table:")
    spark.sql(f"SELECT * FROM {TARGET_FULL_PATH} ORDER BY OrderId, TransactionId").show(10, truncate=False)

    print(f"🎉 Silver to Gold data load complete!")

except Exception as e:
    print(f"❌ Error loading data to Gold table: {str(e)}")
    raise
