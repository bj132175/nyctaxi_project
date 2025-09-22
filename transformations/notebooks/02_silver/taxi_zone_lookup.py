# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType
from datetime import datetime
from delta.tables import DeltaTable

# COMMAND ----------

df = spark.read.format('csv').option('header', True).load('/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv')

# COMMAND ----------

df = df.select(
    col('LocationID').cast(IntegerType()).alias('location_id'),
    col('Borough').alias('borough'),
    col('Zone').alias('zone'),
    col('service_zone').alias('service_zone'),
    current_timestamp().alias('effective_date'),
    lit(None).cast(TimestampType()).alias('end_date')
)

# COMMAND ----------

# Fixed point-in-time used to 'close' any changed active records
# Using a Python timestamp ensures the exact same value is written and can be referenced if needed
end_timestamp = datetime.now()

# Load the SCD2 Delta Table
dt = DeltaTable.forName(spark, 'nyctaxi.02_silver.taxi_zone_lookup')

# COMMAND ----------

# PASS 1: Close any active rows whose tracked attributes changed

# Match only the *active* target rows (end_date IS NULL) with the same business key
# If any tracked column differs, set end_date to end_timestamp to retire that version

dt.alias('t')\
    .merge(
        source=df.alias('s'),
        condition='''
            t.location_id = s.location_id
            AND t.end_date IS NULL
            AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)
        '''
    )\
    .whenMatchedUpdate(
        set={'t.end_date': lit(end_timestamp).cast(TimestampType())}
    )\
    .execute()

# COMMAND ----------

# PASS 2: Insert new current versions

# Insert a row for
#   a) keys we just closed in PASS 1 (no longer an active match)
#   b) brand-new keys not present in the target
# We again match on *active* rows; anything without an active match is inserted

# Get the list of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f'end_date = "{end_timestamp}"').select('location_id').collect()]

# If the list is empty, do nothing
if len(insert_id_list) == 0:
    print('No updated records to insert')
else:
    dt.alias('t')\
        .merge(
            source=df.alias('s'),
            condition=f's.location_id NOT IN ({", ".join(map(str, insert_id_list))})'
        )\
        .whenNotMatchedInsert(
            values={
                't.location_id': 's.location_id',
                't.borough': 's.borough',
                't.zone': 's.zone',
                't.service_zone': 's.service_zone',
                't.effective_date' :current_timestamp(),
                't.end_date': lit(None).cast(TimestampType())
            }
        )\
        .execute()

# COMMAND ----------

# PASS 3: Insert brand-new keys (no historical row in target)

dt.alias('t')\
    .merge(
        source=df.alias('s'),
        condition='t.location_id = s.location_id'
    )\
    .whenNotMatchedInsert(
        values={
            't.location_id': 's.location_id',
            't.borough': 's.borough',
            't.zone': 's.zone',
            't.service_zone': 's.service_zone',
            't.effective_date': current_timestamp(),
            't.end_date': lit(None).cast(TimestampType())
        }
    )\
    .execute()