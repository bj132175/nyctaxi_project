# Databricks notebook source
import urllib.request
import os
import shutil

# COMMAND ----------

try:
    # Define and create the local directory for this date's data
    dir_path = f'/Volumes/nyctaxi/00_landing/data_sources/lookup'
    os.makedirs(dir_path, exist_ok=True)

    # Construct the URL for the parquet file corresponding to this month
    url = f'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
    # Open a connection and stram the remote file
    response = urllib.request.urlopen(url)

    # Define the full path for the downloaded file
    local_path = f'{dir_path}/taxi_zone_lookup.csv'
    # Save the streamed content to the local file in binary mode
    with open(local_path, 'wb') as f:
        shutil.copyfileobj(response, f)
    
    dbutils.jobs.taskValues.set(key='continue_downstream', value='yes')
    print('File successfully uploaded!')
except Exception as e:
    dbutils.jobs.taskValues.set(key='continue_downstream', value='no')
    print(f'File download failed: {str(e)}')