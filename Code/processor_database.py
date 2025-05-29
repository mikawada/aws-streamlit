#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import pandas as pd
import psycopg2
import os
import io
import time
from decimal import Decimal
from tqdm import tqdm
import subprocess

# =================== CONFIGURATION ===================
S3_BUCKET = 'spotify-group3'
S3_OBJECT_KEY = 'raw data/raw_spotify.csv' 

DYNAMODB_TABLE = 'spotify_metadata'

RDS_HOST = "spotify-db.coqlv9bu6y4s.us-east-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DBNAME = "spotify"
RDS_USER = "postgres"
RDS_PASSWORD = "postgres"

AWS_REGION = "us-east-1"
CHUNK_SIZE = 50000  
MAX_RETRIES = 3     
# =======================================================

# =================== HELPER FUNCTIONS ===================
def convert_to_dynamodb_format(item):
    """Recursively convert int/float to Decimal for DynamoDB."""
    if isinstance(item, dict):
        return {k: convert_to_dynamodb_format(v) for k, v in item.items()}
    elif isinstance(item, list):
        return [convert_to_dynamodb_format(v) for v in item]
    elif isinstance(item, (float, int)):
        return Decimal(str(item))
    else:
        return item

def safe_put_item(batch, item):
    """Safe put item into DynamoDB with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            batch.put_item(Item=item)
            return
        except Exception as e:
            print(f"[Warning] Attempt {attempt+1} failed to insert item: {e}")
            time.sleep(2 ** attempt)
    print(f"[Error] Failed to insert item after {MAX_RETRIES} retries.")

def shutdown_instance():
    """Shutdown the EC2 instance."""
    print("Shutting down EC2 instance...")
    subprocess.run(['sudo', 'shutdown', '-h', 'now'])
# ==========================================================

# =================== MAIN PROCESS ===================
print("Initializing clients...")

s3_client = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
meta_table = dynamodb.Table(DYNAMODB_TABLE)

print("Connecting to RDS...")
conn = psycopg2.connect(
    host=RDS_HOST,
    port=RDS_PORT,
    dbname=RDS_DBNAME,
    user=RDS_USER,
    password=RDS_PASSWORD
)
cur = conn.cursor()

# Step 1: Initialize spotify_fact table
cur.execute("""
    DROP TABLE IF EXISTS spotify_fact;
    CREATE TABLE spotify_fact (
        id SERIAL PRIMARY KEY,
        meta_id INT,
        date DATE,
        rank INT,
        region VARCHAR,
        chart VARCHAR,
        trend VARCHAR,
        streams BIGINT
    );
""")
conn.commit()
print("Initialized RDS table: spotify_fact")

# Step 2: Download and Read S3 Data
print("Downloading raw data from S3...")
response = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_OBJECT_KEY)
raw_csv = response['Body'].read()
print(f"Downloaded {len(raw_csv)//1024} KB")

# Step 3: Prepare Variables
meta_seen = {}  # title+artist+url -> meta_id
meta_id_counter = 0
total_meta_inserted = 0
total_fact_inserted = 0

start_time = time.time()

print("Starting chunked processing...")
chunk_iter = pd.read_csv(io.BytesIO(raw_csv), chunksize=CHUNK_SIZE)

for chunk_num, chunk in enumerate(tqdm(chunk_iter, desc="Processing CSV chunks")):
    chunk = chunk[['title', 'artist', 'url', 'date', 'rank', 'region', 'chart', 'trend', 'streams']].copy()

    # --- Build metadata ---
    meta_batch = []
    for idx, row in chunk.iterrows():
        key = (row['title'], row['artist'], row['url'])
        if key not in meta_seen:
            meta_seen[key] = meta_id_counter
            meta_batch.append({
                'meta_id': meta_id_counter,
                'title': row['title'],
		'title_lower': row['title'].lower(),
                'artist': row['artist'],
		'artist_lower': row['artist'].lower(),
                'url': row['url']
            })
            meta_id_counter += 1

    # --- Insert metadata to DynamoDB ---
    with meta_table.batch_writer() as batch:
        for item in meta_batch:
            formatted_item = convert_to_dynamodb_format(item)
            safe_put_item(batch, formatted_item)
    total_meta_inserted += len(meta_batch)

    # --- Merge meta_id back ---
    chunk['meta_id'] = chunk.apply(lambda row: meta_seen[(row['title'], row['artist'], row['url'])], axis=1)

    # --- Prepare fact data ---
    fact_batch = []
    for idx, row in chunk.iterrows():
        streams_value = 0 if pd.isna(row['streams']) else int(row['streams'])
        fact_batch.append((
            int(row['meta_id']),
            row['date'],
            int(row['rank']),
            row['region'],
            row['chart'],
            row['trend'],
            streams_value
        ))

    # --- Insert fact data into RDS ---
    insert_query = """
                    INSERT INTO spotify_fact (meta_id, date, rank, region, chart, trend, streams)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """
    cur.executemany(insert_query, fact_batch)
    conn.commit()

    total_fact_inserted += len(fact_batch)

# Step 4: Close DB Connection
cur.close()
conn.close()

end_time = time.time()
duration = end_time - start_time

print("\n==== Processing Summary ====")
print(f"Total metadata records inserted into DynamoDB: {total_meta_inserted}")
print(f"Total fact records inserted into RDS: {total_fact_inserted}")
print(f"Total time: {duration/60:.2f} minutes")
print("Processing complete.")

# Step 5: Shutdown EC2
shutdown_instance()

