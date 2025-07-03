import pandas as pd
import time
import csv
import os
from collections import Counter
import boto3

# =============================
# 🔧 설정
# =============================
S3_BUCKET = "bookreview-results"
S3_KEY = "cleaned/cleaned_books_100.csv"
LOCAL_FILE = "temp_100.csv"
OUTPUT_FILE = "benchmark_metrics_sequential.csv"
LOADS = [25, 50, 75, 100]  # 데이터 비율 (%)

# =============================
# 📥 S3에서 파일 다운로드
# =============================
def download_from_s3():
    print("\n📥 Downloading data from S3...")
    s3 = boto3.client("s3")
    s3.download_file(S3_BUCKET, S3_KEY, LOCAL_FILE)
    print("✅ complete  Download")

# =============================
# 🔠 워드카운트 함수 (순차적 처리)
# =============================
def word_count(texts):
    counter = Counter()
    for text in texts:
        if isinstance(text, str):
            words = text.lower().split()
            counter.update(words)
    return counter

# =============================
# 😊 감정 분석 카운트 함수 (순차적 처리)
# =============================
def sentiment_count(sentiments):
    counter = Counter()
    for s in sentiments:
        if isinstance(s, str):
            counter[s.lower()] += 1
    return counter

# =============================
# 🏁 실행
# =============================
def run_sequential_tasks():
    download_from_s3()
    df = pd.read_csv(LOCAL_FILE)
    total_rows = len(df)

    results = []

    for pct in LOADS:
        subset = df.iloc[: total_rows * pct // 100]

        # --- 워드카운트 ---
        texts = subset["cleaned_text"].tolist()
        print(f"\n🚀 Sequential WordCount start ({pct}%)")
        start = time.time()
        word_counter = word_count(texts)
        end = time.time()
        elapsed = end - start
        throughput = len(texts) / elapsed
        latency = elapsed / len(texts)
        
        print("=========================================")
        print(f"1. Processing Time: {elapsed:.4f} sec")
        print(f"2. Throughput: {throughput:.2f} records/sec")
        print(f"3. Latency: {latency:.6f} sec/record")

        results.append({
            "type": "sequential",
            "task": "wordcount",
            "percent": pct,
            "records": len(texts),
            "time_sec": round(elapsed, 4),
            "throughput_rps": round(throughput, 2),
            "latency_spr": round(latency, 6)
        })

        # --- 감정 분석 ---
        sentiments = subset["sentiment"].tolist()
        print(f"\n🚀 Sequential Sentiment start ({pct}%)")
        start = time.time()
        sentiment_counter = sentiment_count(sentiments)
        end = time.time()
        elapsed = end - start
        throughput = len(sentiments) / elapsed
        latency = max(elapsed / len(sentiments), 1e-6)
        
        print("=========================================")
        print(f"1. Processing Time: {elapsed:.4f} sec")
        print(f"2. Throughput: {throughput:.2f} records/sec")
        print(f"3. Latency: {latency:.6f} sec/record")

        results.append({
            "type": "sequential",
            "task": "sentiment",
            "percent": pct,
            "records": len(sentiments),
            "time_sec": round(elapsed, 4),
            "throughput_rps": round(throughput, 2),
            "latency_spr": round(latency, 6)
        })

    # 결과 CSV 저장
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print(f"\n✅ complete: {OUTPUT_FILE}")

    # 임시 파일 삭제
    if os.path.exists(LOCAL_FILE):
        os.remove(LOCAL_FILE)
        #print(f"🗑️ 임시 파일 {LOCAL_FILE} 삭제 완료.")

def upload_to_s3(local_file, bucket, s3_key):
    s3 = boto3.client("s3")
    try:
        s3.upload_file(local_file, bucket, s3_key)
        print(f"✅ Uploaded {local_file} to s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")

if __name__ == "__main__":
    run_sequential_tasks()
    upload_to_s3("benchmark_metrics_sequential.csv", "bookreview-results", "hybrid/benchmark_metrics_sequential.csv")
