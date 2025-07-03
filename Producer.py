import boto3
import pandas as pd
import json
import time
import os

STREAM_NAME = "book-reviews-stream"
REGION_NAME = "us-east-1"
S3_BUCKET = "bookreview-results"
BASE_PATH = "/home/ubuntu"

s3 = boto3.client("s3", region_name=REGION_NAME)
kinesis = boto3.client("kinesis", region_name=REGION_NAME)

def send_chunk(df):
    batch = []
    for i, (_, row) in enumerate(df.iterrows()):
        text_val = row.get("cleaned_text", "")
        sentiment_val = row.get("sentiment", "")

        payload = {
            "text": str(text_val) if pd.notna(text_val) else "",
            "sentiment": str(sentiment_val).lower() if pd.notna(sentiment_val) else "",
        }

        batch.append({
            "Data": json.dumps(payload),
            "PartitionKey": f"partition-key"
        })

        if len(batch) == 500:
            kinesis.put_records(StreamName=STREAM_NAME, Records=batch)
            time.sleep(0.2)
            batch.clear()

    if batch:
        kinesis.put_records(StreamName=STREAM_NAME, Records=batch)

def run():
    s3_key = "cleaned/cleaned_books_100.csv"
    local_path = f"{BASE_PATH}/cleaned_books_100.csv"

    print("📥 Downloading dataset from S3...")
    s3.download_file(S3_BUCKET, s3_key, local_path)

    df = pd.read_csv(local_path)
    print(f"📦 Loaded {len(df)} rows. Sending to Kinesis...")

    send_chunk(df)
    print("✅ Data streaming complete.")
    os.remove(local_path)

if __name__ == "__main__":
    run()
