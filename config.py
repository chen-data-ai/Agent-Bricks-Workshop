# Databricks notebook source
# DBTITLE 1,Edit this cell with your resource names
# Change these to your catalog and schema names before running 00_Setup
catalog_name = "agent_bricks_demo"
schema_name = "data_schema"

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
