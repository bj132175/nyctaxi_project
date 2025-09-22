# Databricks notebook source
from pyspark.sql.functions import sum, round, count, col

# COMMAND ----------

df = spark.read.table('nyctaxi.`02_silver`.yellow_trips_enriched')

# COMMAND ----------

df = df.filter('tpep_pickup_datetime < "2025-07-01"')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which vendor makes the most revenue?

# COMMAND ----------

df_vendor = df\
    .groupBy('vendor')\
    .agg(round(sum('total_amount'), 0).alias('total_revenue'))\
    .sort('total_revenue', ascending=False)

df_vendor.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the most popular pickup borough?

# COMMAND ----------

df_pickup = df\
    .groupBy('pickup_borough')\
    .agg(count('pickup_borough').alias('total_pickups'))\
    .sort('total_pickups', ascending=False)

df_pickup.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### What is the most common journey (borough to borough)?

# COMMAND ----------

df_common = df\
    .groupBy('pickup_borough', 'dropoff_borough')\
    .agg(count('pickup_borough').alias('total_pickups'))\
    .sort('total_pickups', ascending=False)

df_common.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a time series chart showing the number of trips and total revenue per day

# COMMAND ----------

df_daily = spark.read.table('nyctaxi.`03_gold`.daily_trip_summary')
df_daily = df_daily\
    .select('pickup_date', 'total_trips', 'total_revenue')\
    .sort('pickup_date', ascending=True)

df_daily.display()

# COMMAND ----------

