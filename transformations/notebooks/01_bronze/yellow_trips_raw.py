# Databricks notebook source
import sys
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), '../..'))

if project_root not in sys.path:
    sys.path.append(project_root)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from datetime import date
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm

# COMMAND ----------

# Obtains the year-month for 2 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyymm(2)

df = spark.read.format('parquet').load(f'/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}')

# COMMAND ----------

df = df.withColumn('processed_timestamp', current_timestamp())

# COMMAND ----------

df.write.mode('append').saveAsTable('nyctaxi.01_bronze.yellow_trips_raw')
