import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import Counter
import multiprocessing as mp
import boto3
import os
import time

# S3에서 full 데이터 다운로드
s3 = boto3.client("s3", region_name="us-east-1")
bucket = "bookreview-results"
key = "cleaned/cleaned_books_100.csv"
local_path = "/home/ubuntu/cleaned_books_full.csv"

try:
    s3.download_file(bucket, key, local_path)
    print("✅ Downloaded full dataset from S3")
except Exception as e:
    print(f"❌ Failed to download: {e}")
    exit(1)

# 전역 저장소
performance = []
sentiment_summary = {}
BASE_PATH = "/home/ubuntu"

# 감정 분석
def count_sentiments(sentiments):
    counter = Counter()
    for s in sentiments:
        if pd.notna(s):
            counter[s.lower()] += 1
    return counter

def merge_counters(results):
    total = Counter()
    for c in results:
        total.update(c)
    return total

# 퍼센트별 처리
def process_sentiment(df_full, percent):
    print(f"\n🔁 Running Sentiment Analysis for {percent}% dataset...")
    subset_len = int(len(df_full) * percent / 100)
    df = df_full.iloc[:subset_len]
    sentiments = df["sentiment"].dropna().tolist()

    start_time = time.time()
    chunk_size = 50000
    chunks = [sentiments[i:i + chunk_size] for i in range(0, len(sentiments), chunk_size)]

    with mp.Pool(mp.cpu_count()) as pool:
        results = pool.map(count_sentiments, chunks)

    combined = merge_counters(results)

    end_time = time.time()
    elapsed = end_time - start_time
    total_rows = len(df)
    throughput = total_rows / elapsed
    latency = (elapsed / total_rows) * 1000

    sentiment_summary[percent] = dict(combined)
    performance.append({
        "percent": percent,
        "time": round(elapsed, 2),
        "throughput": round(throughput, 2),
        "latency": round(latency, 6)
    })

    print(f"⏱️ Time: {elapsed:.2f}s | 📈 Throughput: {throughput:.2f} rows/s | 🕒 Latency: {latency:.6f}s/row")

# 파이차트
def plot_pie_charts(data):
    fig, axs = plt.subplots(2, 2, figsize=(10, 8))
    labels = ["positive", "neutral", "negative"]
    colors = {
        25: ["lightgreen", "lightgray", "salmon"],
        50: ["mediumseagreen", "silver", "tomato"],
        75: ["forestgreen", "darkgray", "indianred"],
        100: ["limegreen", "gainsboro", "firebrick"]
    }
    positions = [(0, 0), (0, 1), (1, 0), (1, 1)]

    for i, percent in enumerate([25, 50, 75, 100]):
        row, col = positions[i]
        counts = [data[percent].get(label, 0) for label in labels]
        axs[row][col].pie(counts, labels=labels, autopct='%1.1f%%',
                          colors=colors[percent], startangle=140,
                          textprops={'fontsize': 10})
        axs[row][col].set_title(f"Sentiment Distribution - {percent}%", fontsize=12)

    fig.suptitle("Sentiment Distribution per Dataset Size", fontsize=14)
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    path = f"{BASE_PATH}/sentiment_pie_summary.png"
    plt.savefig(path)
    plt.close()
    try:
        s3.upload_file(path, bucket, "visualization/sentiment_pie_summary.png")
        print("📤 Uploaded sentiment_pie_summary.png to S3")
        os.remove(path)
    except Exception as e:
        print(f"❌ Failed to upload pie chart: {e}")

# 성능 그래프
def plot_performance(perf_data):
    labels = [f"{d['percent']}%" for d in perf_data]
    x = np.arange(len(labels))
    processing_time = [d["time"] for d in perf_data]
    throughput = [d["throughput"] for d in perf_data]
    latency = [d["latency"] for d in perf_data]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    bars = ax1.bar(x, throughput, color="orchid", width=0.4, label="Throughput (records/sec)")
    ax1.set_ylabel("Throughput (records/sec)", color="blue")
    ax1.set_xlabel("Dataset Load (%)")
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.tick_params(axis="y", labelcolor="blue")
    for bar in bars:
        height = bar.get_height()
        ax1.annotate(f"{height:.2f}", xy=(bar.get_x() + bar.get_width()/2, height),
                     xytext=(0, 3), textcoords="offset points", ha='center', fontsize=8)

    ax2 = ax1.twinx()
    ax2.plot(x, processing_time, color='orange', marker='o', label='Processing Time (s)')
    ax2.plot(x, latency, color='red', marker='s', label='Latency (ms/row)')
    ax2.set_ylabel("Processing Time / Latency", color="black")
    ax2.tick_params(axis="y", labelcolor="black")

    for i in range(len(x)):
        ax2.annotate(f"{processing_time[i]:.2f}", (x[i], processing_time[i]), textcoords="offset points",
                     xytext=(5, 5), fontsize=8, color="orange")
        ax2.annotate(f"{latency[i]:.6f}", (x[i], latency[i]), textcoords="offset points",
                     xytext=(-10, -10), fontsize=8, color="red")

    lines_labels = [ax.get_legend_handles_labels() for ax in [ax1, ax2]]
    lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
    ax1.legend(lines, labels, loc="upper left")

    plt.title("MapReduce Performance Metrics (Sentiment Analysis)")
    plt.tight_layout()
    path = f"{BASE_PATH}/sentiment_performance_summary.png"
    plt.savefig(path)
    plt.close()
    try:
        s3.upload_file(path, bucket, "visualization/sentiment_performance_summary.png")
        print("📤 Uploaded sentiment_performance_summary.png to S3")
        os.remove(path)
    except Exception as e:
        print(f"❌ Failed to upload performance chart: {e}")

# 메인
def main():
    try:
        df = pd.read_csv(local_path)
    except Exception as e:
        print(f"❌ Failed to load input file: {e}")
        return

    for percent in [25, 50, 75, 100]:
        process_sentiment(df, percent)

    plot_pie_charts(sentiment_summary)
    plot_performance(performance)

    try:
        os.remove(local_path)
        print("🧹 Deleted cleaned_books_full.csv to save space.")
    except Exception as e:
        print(f"⚠️ Failed to delete cleaned_books_full.csv: {e}")

if __name__ == "__main__":
    main()
