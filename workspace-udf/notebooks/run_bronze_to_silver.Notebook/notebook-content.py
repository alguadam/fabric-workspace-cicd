# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "63081e19-3045-4aa1-b19f-6425719fe664",
# META       "default_lakehouse_name": "maag_silver",
# META       "default_lakehouse_workspace_id": "82249858-6617-405c-b4be-d40ea331378b",
# META       "known_lakehouses": [
# META         {
# META           "id": "63081e19-3045-4aa1-b19f-6425719fe664"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Processing from Bronze to Silver 

# MARKDOWN ********************

# ## Prepare Clean Environment for Silver Lakehouse 

# CELL ********************

# %run truncate_all_tables_silver

# CELL ********************

# %run drop_all_tables_silver

# MARKDOWN ********************

# ## Create Schema and Tables in Silver 

# CELL ********************

%run model_finance_silver

# CELL ********************

%run model_shared_silver

# CELL ********************

%run model_salesfabric_silver

# CELL ********************

%run model_salesadb_silver

# MARKDOWN ********************

# ## Bronze to Silver - finacne tables 

# CELL ********************

%run bronze_to_silver_finance_account

# CELL ********************

%run bronze_to_silver_finance_invoice

# CELL ********************

%run bronze_to_silver_finance_payment

# MARKDOWN ********************

# ## Bronze to Silver - salesfabric tables 

# CELL ********************

%run bronze_to_silver_salesfabric_order

# CELL ********************

%run bronze_to_silver_salesfabric_orderLine

# CELL ********************

%run bronze_to_silver_salesfabric_orderPayment

# MARKDOWN ********************

# ## Bronze to Silver - salesadb tables 

# CELL ********************

%run bronze_to_silver_salesadb_order

# CELL ********************

%run bronze_to_silver_salesadb_orderLine

# CELL ********************

%run bronze_to_silver_salesadb_orderPayment

# MARKDOWN ********************

# ## Bronze to Silver - shared (customer and product) tables

# CELL ********************

%run bronze_to_silver_shared_customer

# CELL ********************

%run bronze_to_silver_shared_customerAccount

# CELL ********************

%run bronze_to_silver_shared_customerRelationshipType

# CELL ********************

%run bronze_to_silver_shared_customerTradeName

# CELL ********************

%run bronze_to_silver_shared_customerTradeName

# CELL ********************

%run bronze_to_silver_shared_location

# CELL ********************

%run bronze_to_silver_shared_product

# CELL ********************

%run bronze_to_silver_shared_productCategory

# MARKDOWN ********************

