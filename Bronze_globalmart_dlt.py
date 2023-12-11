# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

jdbcHostname ='tred-ag-server.database.windows.net'
jdbcDatabase = 'tred-sql-db'
jdbcPort = "1433"
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbcHostname, jdbcPort, jdbcDatabase)
connectionProperties = {
  "user" : 'tred_admin',
  "password" : 'demo-12-myst',
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

@dlt.create_table(
  comment="The raw customers, ingested from database",
  table_properties={
    "Globalmart_delta.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def customers_raw():
    customers_df = spark.read.jdbc(url=jdbcUrl, table= "customers",properties=connectionProperties)
    return customers_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw vendors, ingested from database",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def vendors_raw():
    vendors_df = spark.read.jdbc(url=jdbcUrl, table= "vendors",properties=connectionProperties)
    return vendors_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw orders, ingested from database",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def orders_raw():
    orders_df = spark.read.jdbc(url=jdbcUrl, table= "orders",properties=connectionProperties)
    return orders_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw transactions, ingested from database",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def transactions_raw():
    transactions_df = spark.read.jdbc(url=jdbcUrl, table= "transactions",properties=connectionProperties)
    return transactions_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw returns, ingested from database",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def returns_raw():
  returns_df = spark.read.jdbc(url=jdbcUrl, table= "returns",properties=connectionProperties)
  return returns_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw products, ingested from data lake",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def products_raw():
  products_df= spark.read.format("json").option("multiLine","true").load("/mnt/globalmart_clean_data/bronze/products (1).json")
  return products_df

# COMMAND ----------

@dlt.create_table(
  comment="The raw customer_reviews, ingested from data lake",
  table_properties={
    "Globalmart_deltaliv.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def reviews_raw():
  customer_reviews_df = spark.read.format("csv").option("header","true").load("/mnt/globalmart_clean_data/bronze/customer_reviews.csv")
  return customer_reviews_df
