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

# # Load Bronze Data to Silver Table - CustomerAccount
# 
# ## Overview
# Load CustomerAccount sample data from Bronze lakehouse files into Silver lakehouse table.
# 
# ## Data Flow
# - **Source**: Bronze Lakehouse /Files/samples_fabric/shared/CustomerAccount_Samples.csv
# - **Target**: Silver Lakehouse shared.CustomerAccount table (Delta table)
# - **Process**: Read CSV, validate schema, load to Delta table
# 
# ---

# CELL ********************

# Step 1 Import Libraries and Set up Source Path 

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

SOURCE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_LAKEHOUSE_ID}/Files/samples_fabric/shared/CustomerAccount_Samples.csv"

TARGET_SCHEMA = "shared"
TARGET_TABLE = "CustomerAccount"
TARGET_FULL_PATH = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"🔄 Loading CustomerAccount data")
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

# Step 2: Validate and conform to target schema

print(f"🔍 Validating data quality...")

# Required columns from Model_Shared_Data.ipynb CustomerAccount table
required_columns = [
    "CustomerAccountId", "ParentAccountId", "CustomerAccountName", "CustomerId", "IsoCurrencyCode", "UpdatedBy"
]

# Only add/populate UpdatedBy if missing
from pyspark.sql import functions as F
if "UpdatedBy" not in df.columns:
    df = df.withColumn("UpdatedBy", F.lit("Source_Data_Loader"))
    print("✅ Added UpdatedBy column with value 'Source_Data_Loader'.")

print(f"✅ Schema reference (required_columns) retained for documentation/model awareness.")

missing_columns = [c for c in required_columns if c not in df.columns]
if missing_columns:
    print(f"⚠️ Warning: Missing columns in source data: {missing_columns}")
else:
    print(f"✅ All required columns present in source data.")

print(f"✅ Schema validation complete (no error raised for missing columns).")

# Data quality checks
null_counts = df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0]
print(f"\n📊 Data Quality Check:")
for col_name in df.columns:
    null_count = null_counts[col_name]
    if null_count > 0:
        print(f"  {col_name}: {null_count} null values")
    else:
        print(f"  {col_name}: ✅ No nulls")

# Show value distributions for CustomerAccountId
print(f"\n🎯 CustomerAccountId Distribution:")
df.groupBy("CustomerAccountId").count().orderBy("CustomerAccountId").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 3: Load data to Silver table

print(f"💾 Loading data to Silver table: {TARGET_FULL_PATH}")

try:
    df.write \
      .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .saveAsTable(TARGET_FULL_PATH)

    print(f"✅ Data loaded successfully to {TARGET_FULL_PATH}")

    # Verify the load
    result_count = spark.sql(f"SELECT COUNT(*) as count FROM {TARGET_FULL_PATH}").collect()[0]["count"]
    print(f"📊 Records in target table: {result_count}")

    # Show sample of loaded data
    print(f"\n📖 Sample from Silver table:")
    spark.sql(f"SELECT * FROM {TARGET_FULL_PATH} ORDER BY CustomerAccountId").show(10, truncate=False)

    print(f"🎉 CustomerAccount data load complete!")

except Exception as e:
    print(f"❌ Error loading data to table: {str(e)}")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
