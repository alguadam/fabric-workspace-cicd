# Fabric notebook source

# METADATA ********************

# META {
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "400ee61d-0fae-4cb3-9d1d-e66011e11d3b",
# META       "default_lakehouse_name": "maag_gold",
# META       "default_lakehouse_workspace_id": "82249858-6617-405c-b4be-d40ea331378b",
# META       "known_lakehouses": [
# META         {
# META           "id": "400ee61d-0fae-4cb3-9d1d-e66011e11d3b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Processing from silver to gold 

# MARKDOWN ********************

# ## Prepare Clean Envronment for gold Lakehouse 

# CELL ********************

# %run truncate_all_tables_gold

# CELL ********************

# %run drop_all_tables_gold

# MARKDOWN ********************

# ## Create Schema and Tables in gold 

# CELL ********************

%run model_finance_gold

# CELL ********************

%run model_shared_gold

# CELL ********************

%run model_salesfabric_gold

# CELL ********************

%run model_salesadb_gold

# MARKDOWN ********************

# ## Silver to gold - finance tables 

# CELL ********************

%run silver_to_gold_finance_account

# CELL ********************

%run silver_to_gold_finance_invoice

# CELL ********************

%run silver_to_gold_finance_payment

# MARKDOWN ********************

# ## Silver to gold - salesfabric tables 

# CELL ********************

%run silver_to_gold_salesfabric_order

# CELL ********************

%run silver_to_gold_salesfabric_orderLine

# CELL ********************

%run silver_to_gold_salesfabric_orderPayment

# MARKDOWN ********************

# ## Silver to gold - salesadb tables 

# CELL ********************

%run silver_to_gold_salesadb_order

# CELL ********************

%run silver_to_gold_salesadb_orderLine

# CELL ********************

%run silver_to_gold_salesadb_orderPayment

# MARKDOWN ********************

# ## Silver to gold - shared (customer and product) tables

# CELL ********************

%run silver_to_gold_shared_customer

# CELL ********************

%run silver_to_gold_shared_customerAccount

# CELL ********************

%run silver_to_gold_shared_customerRelationshipType

# CELL ********************

%run silver_to_gold_shared_customerTradeName

# CELL ********************

%run silver_to_gold_shared_customerTradeName

# CELL ********************

%run silver_to_gold_shared_location

# CELL ********************

%run silver_to_gold_shared_product

# CELL ********************

%run silver_to_gold_shared_productCategory

# MARKDOWN ********************

