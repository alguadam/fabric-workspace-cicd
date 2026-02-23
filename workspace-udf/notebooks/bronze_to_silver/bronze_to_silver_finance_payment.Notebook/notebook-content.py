# Fabric notebook source

# METADATA ********************

# META {
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

# # Load Bronze Data to Silver Table - Payment
# 
# ## Overview
# Load Payment sample data from Bronze lakehouse files into Silver lakehouse table 
# 
# ## Data Flow
# - **Source-Fabric**: Bronze Lakehouse/Files/samples_fabric/finance/Payment_Samples_Fabric.csv
# - **Source-ADB**: Bronze Lakehouse/Files/samples_databricks/finance/Payment_Samples_ADB.csv
# - **Target**: Silver Lakehouse.finance.Payment table (or any attached default lakehouse)
# - **Process**: Read CSV, validate schema, check data quality, show value distributions, load to Delta table, verify load
# 
# ---

# CELL ********************

# --- Fabric Channel ---
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import functions as F
import sempy.fabric as fabric

# Configuration - Using correct Fabric cross-lakehouse path from Fabric portal
# Get workspace ID dynamically at runtime (avoids issues with spaces in workspace names)
WORKSPACE_ID = fabric.get_notebook_workspace_id()

# Get lakehouse ID dynamically (avoids issues with lakehouse names)
lakehouse_properties = mssparkutils.lakehouse.get("maag_bronze")
SOURCE_LAKEHOUSE_ID = lakehouse_properties.id

FABRIC_SOURCE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_LAKEHOUSE_ID}/Files/samples_fabric/finance/Payment_Samples_Fabric.csv"

TARGET_SCHEMA = "finance"
TARGET_TABLE = "payment"
TARGET_FULL_PATH = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"🔄 Loading Fabric Payment data")
print(f"📂 Source: {FABRIC_SOURCE_PATH}")
print(f"🎯 Target: {TARGET_FULL_PATH}")

# Read CSV from Bronze lakehouse
payment_df = spark.read.option("header", "true").option("inferSchema", "true").csv(FABRIC_SOURCE_PATH)

print(f"✅ Data loaded successfully")
print(f"📊 Records: {payment_df.count()}")
print(f"📋 Columns: {payment_df.columns}")

# Display sample data
print(f"\n📖 Sample data:")
payment_df.show(10, truncate=False)

required_columns = [
    "PaymentId", "PaymentNumber", "InvoiceId", "OrderId", "PaymentDate", "PaymentAmount",
    "PaymentStatus", "PaymentMethod", "CreatedBy"
 ]

missing_columns = [c for c in required_columns if c not in payment_df.columns]
if missing_columns:
    print(f"⚠️ Warning: Missing columns in source data: {missing_columns}")
else:
    print(f"✅ All required columns present in source data.")

for col_name in missing_columns:
    if col_name == "PaymentAmount":
        payment_df = payment_df.withColumn(col_name, F.lit(0.0))
    else:
        payment_df = payment_df.withColumn(col_name, F.lit(""))

payment_df = payment_df.withColumn("PaymentId", col("PaymentId").cast(StringType()))
payment_df = payment_df.withColumn("PaymentNumber", col("PaymentNumber").cast(StringType()))
payment_df = payment_df.withColumn("InvoiceId", col("InvoiceId").cast(StringType()))
payment_df = payment_df.withColumn("OrderId", col("OrderId").cast(StringType()))
payment_df = payment_df.withColumn("PaymentDate", col("PaymentDate").cast(DateType()))
payment_df = payment_df.withColumn("PaymentAmount", col("PaymentAmount").cast(DoubleType()))
payment_df = payment_df.withColumn("PaymentStatus", col("PaymentStatus").cast(StringType()))
payment_df = payment_df.withColumn("PaymentMethod", col("PaymentMethod").cast(StringType()))
payment_df = payment_df.withColumn("CreatedBy", col("CreatedBy").cast(StringType()))
payment_df = payment_df.select(required_columns)

# Data quality checks
print(f"\n📊 Data Quality Check:")
null_counts = payment_df.select([F.sum(col(c).isNull().cast("int")).alias(c) for c in required_columns]).collect()[0]
for col_name in required_columns:
    null_count = null_counts[col_name]
    if null_count > 0:
        print(f"  {col_name}: {null_count} null values")
    else:
        print(f"  {col_name}: ✅ No nulls")

# Show value distributions for PaymentStatus
print(f"\n🎯 PaymentStatus Distribution:")
payment_df.groupBy("PaymentStatus").count().orderBy("PaymentStatus").show()

# Ensure the target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# Load data to Silver table
print(f"💾 Loading data to Silver table: {TARGET_FULL_PATH}")
try:
    payment_df.write \
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
    spark.sql(f"SELECT * FROM {TARGET_FULL_PATH} ORDER BY PaymentId").show(10, truncate=False)
    print(f"🎉 Payment data load complete!")
except Exception as e:
    print(f"❌ Error loading data to table: {str(e)}")
    raise
# --- End Fabric Channel ---

# CELL ********************

# --- ADB Channel ---
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum as spark_sum
from pyspark.sql import functions as F
import sempy.fabric as fabric

# Configuration - Using correct Fabric cross-lakehouse path from Fabric portal
# Get workspace ID dynamically at runtime (avoids issues with spaces in workspace names)
WORKSPACE_ID = fabric.get_notebook_workspace_id()

# Get lakehouse ID dynamically (avoids issues with lakehouse names)
lakehouse_properties = mssparkutils.lakehouse.get("maag_bronze")
SOURCE_LAKEHOUSE_ID = lakehouse_properties.id

ADB_SOURCE_PATH = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{SOURCE_LAKEHOUSE_ID}/Files/samples_fabric/finance/Payment_Samples_ADB.csv"

TARGET_SCHEMA = "finance"
TARGET_TABLE = "payment"
TARGET_FULL_PATH = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

print(f"🔄 Loading ADB Payment data")
print(f"📂 Source: {ADB_SOURCE_PATH}")
print(f"🎯 Target: {TARGET_FULL_PATH}")

# Read CSV from Bronze lakehouse
payment_df = spark.read.option("header", "true").option("inferSchema", "true").csv(ADB_SOURCE_PATH)

print(f"✅ Data loaded successfully")
print(f"📊 Records: {payment_df.count()}")
print(f"📋 Columns: {payment_df.columns}")

# Display sample data
print(f"\n📖 Sample data:")
payment_df.show(10, truncate=False)

required_columns = [
    "PaymentId", "PaymentNumber", "InvoiceId", "OrderId", "PaymentDate", "PaymentAmount",
    "PaymentStatus", "PaymentMethod", "CreatedBy"
 ]

missing_columns = [c for c in required_columns if c not in payment_df.columns]
if missing_columns:
    print(f"⚠️ Warning: Missing columns in source data: {missing_columns}")
else:
    print(f"✅ All required columns present in source data.")

for col_name in missing_columns:
    if col_name == "PaymentAmount":
        payment_df = payment_df.withColumn(col_name, F.lit(0.0))
    else:
        payment_df = payment_df.withColumn(col_name, F.lit(""))

payment_df = payment_df.withColumn("PaymentId", col("PaymentId").cast(StringType()))
payment_df = payment_df.withColumn("PaymentNumber", col("PaymentNumber").cast(StringType()))
payment_df = payment_df.withColumn("InvoiceId", col("InvoiceId").cast(StringType()))
payment_df = payment_df.withColumn("OrderId", col("OrderId").cast(StringType()))
payment_df = payment_df.withColumn("PaymentDate", col("PaymentDate").cast(DateType()))
payment_df = payment_df.withColumn("PaymentAmount", col("PaymentAmount").cast(DoubleType()))
payment_df = payment_df.withColumn("PaymentStatus", col("PaymentStatus").cast(StringType()))
payment_df = payment_df.withColumn("PaymentMethod", col("PaymentMethod").cast(StringType()))
payment_df = payment_df.withColumn("CreatedBy", col("CreatedBy").cast(StringType()))
payment_df = payment_df.select(required_columns)

# Data quality checks
print(f"\n📊 Data Quality Check:")
null_counts = payment_df.select([F.sum(col(c).isNull().cast("int")).alias(c) for c in required_columns]).collect()[0]
for col_name in required_columns:
    null_count = null_counts[col_name]
    if null_count > 0:
        print(f"  {col_name}: {null_count} null values")
    else:
        print(f"  {col_name}: ✅ No nulls")

# Show value distributions for PaymentStatus
print(f"\n🎯 PaymentStatus Distribution:")
payment_df.groupBy("PaymentStatus").count().orderBy("PaymentStatus").show()

# Ensure the target schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}")

# Load data to Silver table
print(f"💾 Loading data to Silver table: {TARGET_FULL_PATH}")
try:
    payment_df.write \
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
    spark.sql(f"SELECT * FROM {TARGET_FULL_PATH} ORDER BY PaymentId").show(10, truncate=False)
    print(f"🎉 Payment data load complete!")
except Exception as e:
    print(f"❌ Error loading data to table: {str(e)}")
    raise
# --- End ADB Channel ---
