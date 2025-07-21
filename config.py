# Databricks notebook source
#%pip install --upgrade --quiet databricks-sdk 

# COMMAND ----------

#dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Edit this cell with your resource names
#catalog_name = "agent_bricks_demo"
#schema_name = "data_schema"

catalog_name = "users"
schema_name = "karthiga_mahali"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
