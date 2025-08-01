# Databricks notebook source
# DBTITLE 1,Edit this cell with your resource names
catalog_name = "agent_bricks_demo"
schema_name = "data_schema"


# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
