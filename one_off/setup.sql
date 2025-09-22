-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Catalog

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS nyctaxi
MANAGED LOCATION 'abfss://unity-catalog-storage@dbstoragezdqy65iuwruaq.dfs.core.windows.net/885784623398434';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Schemas

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS nyctaxi.00_landing;
CREATE SCHEMA IF NOT EXISTS nyctaxi.01_bronze;
CREATE SCHEMA IF NOT EXISTS nyctaxi.02_silver;
CREATE SCHEMA IF NOT EXISTS nyctaxi.03_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Volume

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS nyctaxi.00_landing.data_sources;