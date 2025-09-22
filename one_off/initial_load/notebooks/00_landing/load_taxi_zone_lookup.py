# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

dir_path = f'/Volumes/nyctaxi/00_landing/data_sources/lookup'
os.makedirs(dir_path, exist_ok=True)

url = f'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
response = urllib.request.urlopen(url)

local_path = f'{dir_path}/taxi_zone_lookup.csv'
with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)