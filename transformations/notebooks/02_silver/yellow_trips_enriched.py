# Databricks notebook source
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# Get the first day of the month two months ago
two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

# COMMAND ----------

# Load taxi zone lookup data from the silver layer
df_lookup = spark.read.table('nyctaxi.`02_silver`.taxi_zone_lookup')

# Load cleansed yellow taxi trip data from the silver layer
# and filter to only include trips with a pickup datetime later than the start date from two months ago
df_trips = spark.read.table('nyctaxi.`02_silver`.yellow_trips_cleansed')\
            .filter(f'tpep_pickup_datetime > "{two_months_ago_start}"')

# COMMAND ----------

# Join trips with pickup zone details (borough and zone name)
df_join_1 = df_trips.join(
                df_lookup, 
                df_trips.pickup_location_id == df_lookup.location_id,
                "left"
                ).select(
                    df_trips.vendor,
                    df_trips.tpep_pickup_datetime,
                    df_trips.tpep_dropoff_datetime,
                    df_trips.trip_duration,
                    df_trips.passenger_count,
                    df_trips.trip_distance,
                    df_trips.rate_type,
                    df_lookup.borough.alias("pickup_borough"),
                    df_lookup.zone.alias("pickup_zone"),
                    df_trips.dropoff_location_id,
                    df_trips.payment_type,
                    df_trips.fare_amount,
                    df_trips.extra,
                    df_trips.mta_tax,
                    df_trips.tolls_amount,
                    df_trips.improvement_surcharge,
                    df_trips.total_amount,
                    df_trips.congestion_surcharge,
                    df_trips.airport_fee,  
                    df_trips.cbd_congestion_fee,
                    df_trips.processed_timestamp
                )

# COMMAND ----------

# Join the result with dropoff zone details to complete enrichment
df_join_final = df_join_1.join(
                                df_lookup, 
                                df_join_1.dropoff_location_id == df_lookup.location_id,
                                "left"
                                ).select(
                                            df_join_1.vendor,
                                            df_join_1.tpep_pickup_datetime,
                                            df_join_1.tpep_dropoff_datetime,
                                            df_trips.trip_duration,
                                            df_join_1.passenger_count,
                                            df_join_1.trip_distance,
                                            df_join_1.rate_type,
                                            df_join_1.pickup_borough,
                                            df_lookup.borough.alias("dropoff_borough"),
                                            df_join_1.pickup_zone,
                                            df_lookup.zone.alias("dropoff_zone"),
                                            df_join_1.payment_type,
                                            df_join_1.fare_amount,
                                            df_join_1.extra,
                                            df_join_1.mta_tax,
                                            df_join_1.tolls_amount,
                                            df_join_1.improvement_surcharge,
                                            df_join_1.total_amount,
                                            df_join_1.congestion_surcharge,
                                            df_join_1.airport_fee,  
                                            df_join_1.cbd_congestion_fee,
                                            df_join_1.processed_timestamp
                                )

# COMMAND ----------

df_join_final.write.mode("append").saveAsTable("nyctaxi.02_silver.yellow_trips_enriched")