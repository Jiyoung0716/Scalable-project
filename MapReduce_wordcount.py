import pandas as pd
import multiprocessing as mp
from collections import Counter
import re
import boto3
import time
import os
import matplotlib.pyplot as plt
import numpy as np


s3 = boto3.client('s3', region_name='us-east-1')
S3_BUCKET_NAME = "bookreview-results"

# 텍스트 전처리
def tokenize(text):
    text = str(text).lower()
    text = re.sub(r'[^a-z\s]', '', text)
    return text.split()

def count_words(texts):
    counter = Counter()
    for text in texts:
        tokens = tokenize(text)
        counter.update(tokens)
    return counter

# Top-N 병합
def merge_top_n_counters(results, top_n=10, local_top_k=100):
    temp = Counter()
    for partial in results:
        top_local = partial.most_common(local_top_k)
        temp.update(dict(top_local))
    return temp.most_common(top_n)

# 결과 저장용 전역 변수
performance = []
top_words_all = {}

# Top 10 단어 그래프 (2x2)
def plot_all_top_words(word_data):
    colors = {25: 'skyblue', 50: 'lightgreen', 75: 'lightcoral', 100: 'plum'}
    fig, axs = plt.subplots(2, 2, figsize=(14, 10))
    axs = axs.flatten()
    for idx, percent in enumerate(sorted(word_data.keys())):
        words, counts = zip(*word_data[percent])
        axs[idx].bar(words, counts, color=colors.get(percent, 'gray'), edgecolor='black')
        axs[idx].set_title(f"Top 10 Words - {percent}%", fontsize=12)
        axs[idx].set_ylabel("Count")
        axs[idx].tick_params(axis='x', rotation=45)
        for i, count in enumerate(counts):
            axs[idx].text(i, count + 1, str(count), ha='center', fontsize=9)
    fig.suptitle("Word Count Top 10 per Dataset Size", fontsize=16)
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)

    img_path = "/home/ubuntu/top_words_summary.png"
    plt.savefig(img_path)
    plt.close()

    try:
        s3.upload_file(img_path, S3_BUCKET_NAME, "visualization/top_words_summary.png")
        print("📤 Uploaded top_words_summary.png to S3")
    except Exception as e:
        print(f"❌ Failed to upload top_words_summary.png: {e}")
    try:
        os.remove(img_path)
    except:
        pass

def plot_performance(perf_data):
    labels = [f"{d['percent']}%" for d in perf_data]
    x = np.arange(len(labels))
    processing_time = [d['time'] for d in perf_data]
    throughput = [d['throughput'] for d in perf_data]
    latency = [d['latency'] for d in perf_data]

    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    # Bar: Throughput
    bars = ax1.bar(x, throughput, color='lightblue', width=0.4, label='Throughput (records/sec)')
    ax1.set_ylabel('Throughput (records/sec)', color='blue')
    ax1.set_xlabel('Dataset Load (%)')
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels)
    ax1.tick_params(axis='y', labelcolor='blue')
    for bar in bars:
        height = bar.get_height()
        ax1.annotate(f'{height:.2f}', xy=(bar.get_x() + bar.get_width() / 2, height),
                     xytext=(0, 3), textcoords="offset points", ha='center', fontsize=8)
    
    # Line: Processing Time
    ax2 = ax1.twinx()
    ax2.plot(x, processing_time, color='orange', marker='o', linewidth=2, label='Processing Time (s)')
    
    # Line: Latency
    ax2.plot(x, latency, color='red', marker='s', linewidth=2, label='Latency (ms/row)')
    ax2.set_ylabel('Processing Time / Latency', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    
    # Annotate points
    for i, val in enumerate(processing_time):
        ax2.annotate(f'{val:.2f}', xy=(x[i], processing_time[i]), xytext=(5, 5),
                     textcoords='offset points', fontsize=8, color='orange')
    for i, val in enumerate(latency):
        ax2.annotate(f'{val:.6f}', xy=(x[i], latency[i]), xytext=(-5, -10),
                     textcoords='offset points', fontsize=8, color='red')

    # Legend
    lines_labels = [ax.get_legend_handles_labels() for ax in [ax1, ax2]]
    lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]
    ax1.legend(lines, labels, loc='upper left')

    plt.title('MapReduce Performance Metrics by Dataset Size')
    plt.tight_layout()

    img_path = "/home/ubuntu/performance_summary.png"
    plt.savefig(img_path)
    plt.close()

    try:
        s3.upload_file(img_path, S3_BUCKET_NAME, "visualization/performance_summary.png")
        print("📤 Uploaded performance_summary.png to S3")
    except Exception as e:
        print(f"❌ Failed to upload performance_summary.png: {e}")
    try:
        os.remove(img_path)
    except:
        pass


def process_wordcount(percent):
    print(f"\n🔁 Running WordCount for {percent}% dataset...")
    s3_input_key = f"cleaned/cleaned_books_{percent}.csv"
    base_path = "/home/ubuntu"
    local_input = f"{base_path}/temp_input_{percent}.csv"

    try:
        s3.download_file(S3_BUCKET_NAME, s3_input_key, local_input)
    except Exception as e:
        print(f"❌ Failed to download {s3_input_key}: {e}")
        return

    df = pd.read_csv(local_input)
    texts = df['cleaned_text'].dropna().tolist()

    start_time = time.time()
    num_processes = mp.cpu_count()
    chunk_size = 50000
    chunks = [texts[i:i + chunk_size] for i in range(0, len(texts), chunk_size)]

    with mp.Pool(processes=num_processes) as pool:
        results = pool.map(count_words, chunks)

    top_words = merge_top_n_counters(results, top_n=10)
    end_time = time.time()

    elapsed = end_time - start_time
    total_rows = len(df)
    throughput = total_rows / elapsed
    latency_ms = (elapsed / total_rows) * 1000

    performance.append({
        "percent": percent,
        "time": round(elapsed, 2),
        "throughput": round(throughput, 2),
        "latency": round(latency_ms, 6)
    })
    
    print(f"⏱️ Time: {elapsed:.2f}s | 📈 Throughput: {throughput:.2f} rows/s | 🕒 Latency: {latency_ms:.6f}s/row")

    top_words_all[percent] = top_words

    try:
        os.remove(local_input)
    except:
        pass


def main():
    for percent in [25, 50, 75, 100]:
        process_wordcount(percent)
    plot_performance(performance)
    plot_all_top_words(top_words_all)

if __name__ == "__main__":
    main()