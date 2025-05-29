import streamlit as st
import pandas as pd
import requests
from PIL import Image
import datetime
import plotly.express as px
import psycopg2
import boto3
from boto3.dynamodb.conditions import Attr,Key

# --- Streamlit Page Setup ---
st.set_page_config(page_title="Spotify Chart Trends", layout="wide")

# --- Spotify Logo and Title ---
logo = Image.open('image/spotify.png')
st.image(logo, width=200)  # Bigger logo at top
st.title("🎵 Spotify Chart Trends Dashboard")
st.caption("Analyze top streaming songs across regions and dates!")

# --- API Information ---
API_URL = "https://bbj5gwcikk.execute-api.us-east-1.amazonaws.com/search"

# RDS
RDS_HOST = "spotify-db.coqlv9bu6y4s.us-east-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DBNAME = "spotify"
RDS_USER = "postgres"
RDS_PASSWORD = "postgres"

def query_rds(region, date):
    conn = psycopg2.connect(
        host=RDS_HOST,
        port=RDS_PORT,
        dbname=RDS_DBNAME,
        user=RDS_USER,
        password=RDS_PASSWORD
    )
    query = """
        SELECT *
        FROM spotify_fact
        WHERE region = %s
          AND date = %s
    """
    cursor = conn.cursor()
    cursor.execute(query, (region, date))
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    cursor.close()
    conn.close()
    return df


    
# DynamoDB
DYNAMODB_TABLE = 'spotify_metadata'
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
dynamo_table = dynamodb.Table(DYNAMODB_TABLE)
dynamodb_client = dynamodb.meta.client 

def query_dynamodb(meta_id_list):
    meta_id_list_dedup = list(set(meta_id_list))
    batch_size = 100
    all_items = []

    for i in range(0, len(meta_id_list_dedup), batch_size):
        batch_meta_ids = meta_id_list_dedup[i:i+batch_size]
        keys = [{'meta_id': meta_id} for meta_id in batch_meta_ids] 

        response = dynamodb_client.batch_get_item(
            RequestItems={
                DYNAMODB_TABLE: {
                    'Keys': keys
                }
            }
        )

        items = response['Responses'].get(DYNAMODB_TABLE, [])
        all_items.extend(items)

    # DataFrame
    metadata_df = pd.DataFrame([{
        'meta_id': int(item['meta_id']),
        'artist': item['artist'],
        'title': item['title'],
        'url': item.get('url', '')
    } for item in all_items])

    return metadata_df



# --- UI: User Selections ---
available_regions = ['United States', 'United Kingdom', 'Canada', 'Australia', 'Germany']
region = st.selectbox("Select a Region", available_regions)

selected_date = st.date_input(
    "Select a Date",
    min_value=datetime.date(2017, 1, 1),
    max_value=datetime.date(2021, 12, 31),
    value=datetime.date(2017, 1, 1)
)
date = selected_date.strftime('%Y-%m-%d')

# --- Button to Trigger API Call ---
if st.button("Get Top Songs"):
    params = {
        'region': region,
        'date': date
    }
    
    try:
        df = query_rds(region, date)
        
        if df.empty:
            st.warning("No songs found for this region and date.")
        else:
            meta_ids = df['meta_id'].tolist()
            metadata_df = query_dynamodb(meta_ids)
            
            if not metadata_df.empty:
                df = df.merge(metadata_df, on='meta_id', how='left')
        
        if not df.empty:
            
            st.success(f"Showing {len(df)} songs for {region} on {date}")

            # --- Key Statistics ---
            total_streams = df['streams'].sum()
            unique_artists = df['artist'].nunique()
            avg_streams = int(df['streams'].mean())
            most_streamed_song = df.sort_values(by='streams', ascending=False).iloc[0]['title']

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(label="🎶 Total Streams", value=f"{total_streams:,}")

            with col2:
                st.metric(label="🎤 Unique Artists", value=unique_artists)

            with col3:
                st.metric(label="🎧 Avg Streams per Song", value=f"{avg_streams:,}")

            with col4:
                st.metric(label="🥇 Top Song", value=most_streamed_song)

            # --- Visualizations ---

            # Top 10 streamed songs (horizontal bar chart)
            st.subheader("Top 10 Songs by Streams")
            top10 = df.sort_values(by='streams', ascending=False).head(10)
            fig_bar = px.bar(
                top10,
                x='streams',
                y='title',
                orientation='h',
                text='streams',
                labels={'streams': 'Streams', 'title': 'Song Title'},
                title='Top 10 Streamed Songs',
                color_discrete_sequence=['#1DB954']  # Spotify green
            )
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)

            # Top 10 artists by total streams
            st.subheader("Top 10 Artists by Total Streams")
            top_artists = df.groupby('artist')['streams'].sum().sort_values(ascending=False).head(10).reset_index()
            fig_artist = px.bar(
                top_artists,
                x='artist',
                y='streams',
                text='streams',
                labels={'streams': 'Streams', 'artist': 'Artist'},
                title='Top 10 Artists by Streams',
                color_discrete_sequence=['#1DB954']  # Spotify green
            )
            st.plotly_chart(fig_artist, use_container_width=True)

            # --- Raw Data Preview ---
            st.subheader("Raw Song Data")
            st.dataframe(df)
        else:
            st.warning("No songs found for this region and date.")
       
            
    except Exception as e:
        st.error(f"An error occurred: {e}")

# --- Footer ---
st.markdown("---")
st.caption("Built using AWS S3, Lambda, API Gateway, and Streamlit.")
