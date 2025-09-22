# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

df = spark.read.table('nyctaxi.`02_silver`.yellow_trips_enriched')

# COMMAND ----------

df = df\
        .groupBy(df.tpep_pickup_datetime.cast('date').alias('pickup_date'))\
        .agg(
            count('*').alias('total_trips'),
            round(avg('passenger_count'), 1).alias('avg_passengers'),
            round(avg('trip_distance'), 1).alias('avg_distance'),
            round(avg('fare_amount'), 1).alias('avg_tip_fare'),
            max('fare_amount').alias('max_fare'),
            min('fare_amount').alias('min_fare'),
            round(sum('total_amount'), 2).alias('total_revenue')
        )

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyctaxi.`03_gold`.daily_trip_summary')