# Databricks notebook source
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# COMMAND ----------

import pandas as pd
df = pd.read_csv('./data/synthetic_car_data.csv')
spark_df = spark.createDataFrame(df)
spark_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.synthetic_car_data")

# COMMAND ----------

import zipfile
import os

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.`tech_support`")

current_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
source_path = f"file:/Workspace/{current_dir}/data"

dbutils.fs.cp(f"{source_path}/tech_support/", f"/Volumes/{catalog_name}/{schema_name}/tech_support", recurse=True)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.`dip`")
dbutils.fs.cp(f"{source_path}/dip.png/", f"/Volumes/{catalog_name}/{schema_name}/dip")

# COMMAND ----------

# MAGIC %md
# MAGIC
