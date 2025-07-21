# Databricks notebook source
#%pip install --upgrade --quiet databricks-sdk lxml langchain databricks-vectorsearch cloudpickle openai pypdf llama_index langgraph==0.3.4 sqlalchemy openai mlflow mlflow[databricks] langchain_community databricks-agents databricks-langchain uv torch databricks-connect==16.1.* markdownify ftfy

%pip install --upgrade --quiet databricks-sdk 

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Edit this cell with your resource names
catalog_name = "users"
schema_name = "te_chen"
