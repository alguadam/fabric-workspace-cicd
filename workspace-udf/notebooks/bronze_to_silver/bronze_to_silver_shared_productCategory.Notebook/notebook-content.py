# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6677c554-f58d-4371-81a4-c7b5a53648b7",
# META       "default_lakehouse_name": "maag_silver",
# META       "default_lakehouse_workspace_id": "82249858-6617-405c-b4be-d40ea331378b",
# META       "known_lakehouses": [
# META         {
# META           "id": "6677c554-f58d-4371-81a4-c7b5a53648b7"
# META         },
# META         {
# META           "id": "63081e19-3045-4aa1-b19f-6425719fe664"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load Bronze Data to Table - ProductCategory
# 
# ## Overview
# Load ProductCategory data from the Bronze CSV file to the Delta table in the lakehouse.
# 
# ## Data Flow
# - **Source**: Bronze Lakehouse /Files/samples_fabric/shared/ProductCategory_Samples_Combined.csv
# - **Target**: Silver Lakehouse shared.ProductCategory (Delta table)
# - **Process**: Read CSV, validate schema, load to Delta table

# CELL ********************

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum
import os
import sempy.fabric as fabric

# Configuration - Using correct Fabric cross-lakehouse path from Fabric portal
# Get workspace ID dynamically at runtime (avoids issues with spaces in workspace names)
WORKSPACE_ID = fabric.get_notebook_workspace_id()

# Get lakehouse ID dynamically (avoids issues with lakehouse names)
lakehouse_properties = mssparkutils.lakehouse.get("maag_bronze")
SOURCE_LAKEHOUSE_ID = lakehouse_properties.id

SOURCE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_LAKEHOUSE_ID}/Files/samples_fabric/shared/ProductCategory_Samples_Combined.csv"

TARGET_SCHEMA = "shared"
TARGET_TABLE = "ProductCategory"
TARGET_FULL_PATH = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"🔄 Loading ProductCategory data")
print(f"📂 Source: {SOURCE_PATH}")
print(f"🎯 Target: {TARGET_FULL_PATH}")

# Read CSV from Bronze lakehouse
df = spark.read.option("header", "true").option("inferSchema", "true").csv(SOURCE_PATH)

print(f"✅ Data loaded successfully")
print(f"📊 Records: {df.count()}")
print(f"📋 Columns: {df.columns}")

# Display sample data
print(f"\n📖 Sample data:")
df.show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data quality check: Nulls in key columns
nulls = df.select(
    spark_sum(col("CategoryId").isNull().cast("int")).alias("null_CategoryId"),
    spark_sum(col("CategoryName").isNull().cast("int")).alias("null_CategoryName"),
    spark_sum(col("IsActive").isNull().cast("int")).alias("null_IsActive")
).collect()[0]

print(f"🔍 Null check results:")
print(f"  CategoryId nulls:   {nulls['null_CategoryId']}")
print(f"  CategoryName nulls: {nulls['null_CategoryName']}")
print(f"  IsActive nulls:     {nulls['null_IsActive']}")

if nulls['null_CategoryId'] > 0 or nulls['null_CategoryName'] > 0 or nulls['null_IsActive'] > 0:
    print(f"⚠️ Warning: Nulls found in key columns!")
else:
    print(f"✅ No nulls in key columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write DataFrame to Delta table (overwrite mode)
print(f"💾 Writing data to Delta table: {TARGET_FULL_PATH}")

df.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(TARGET_FULL_PATH)

print(f"✅ Data written to Delta table")

# Verify the load
result_count = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_FULL_PATH}").collect()[0]["count"]
print(f"📊 Records in Delta table: {result_count}")

print(f"\n📖 Sample from Delta table:")
spark.sql(f"SELECT * FROM {TARGET_FULL_PATH}").show(10, truncate=False)

print(f"🎉 Bronze to Delta table load complete!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
