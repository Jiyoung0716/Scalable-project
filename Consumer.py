import matplotlib.pyplot as plt
import boto3
import json
import time
from collections import deque, Counter
from datetime import datetime, timedelta, timezone
from multiprocessing import Process, Manager

# AWS Kinesis 설정
REGION_NAME = "us-east-1"
STREAM_NAME = "book-reviews-stream"

# 슬라이딩 윈도우 설정
WINDOW_SECONDS = 180  # 윈도우 유지 시간 (초)
SLIDING_INTERVAL_SECONDS = 5  # 5초마다 갱신

# 텍스트를 소문자 단어 리스트로 변환
def tokenize(text):
    if not isinstance(text, str):
        return []
    return text.lower().split()

# Kinesis에서 데이터를 가져오는 프로세스
def consume_data(shared_window):
    kinesis = boto3.client("kinesis", region_name=REGION_NAME)
    shards = kinesis.describe_stream(StreamName=STREAM_NAME)["StreamDescription"]["Shards"]

    shard_iterators = []
    error_printed = set()  # 에러 메시지 중복 방지용

    for shard in shards:
        try:
            iterator = kinesis.get_shard_iterator(
                StreamName=STREAM_NAME,
                ShardId=shard["ShardId"],
                ShardIteratorType="LATEST"
            )["ShardIterator"]
            shard_iterators.append((shard["ShardId"], iterator))
        except Exception as e:
            print(f"❌ Failed to get shard iterator for {shard['ShardId']}: {e}")

    while True:
        for idx, (shard_id, iterator) in enumerate(shard_iterators):
            try:
                if not iterator:
                    if shard_id not in error_printed:
                        print(f"⚠️ Skipping shard {shard_id}: No iterator")
                        error_printed.add(shard_id)
                    continue

                response = kinesis.get_records(ShardIterator=iterator, Limit=100)
                shard_iterators[idx] = (shard_id, response.get("NextShardIterator"))
                now = datetime.now(timezone.utc)

                for record in response.get("Records", []):
                    try:
                        data = json.loads(record["Data"])
                        text = data.get("text", "")
                        sentiment = data.get("sentiment", "")
                        words = tokenize(text)
                        shared_window.append((now, words, sentiment))
                    except:
                        continue

            except Exception as e:
                if shard_id not in error_printed:
                    print(f"❌ Error on shard {shard_id}: {e}")
                    error_printed.add(shard_id)
        time.sleep(0.5)

# 시각화 업데이트 함수
def run_visualization(shared_window):
    plt.ion()
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Amazon Book Review - Real-time Analysis", fontsize=16, fontweight='bold')
    last_update = datetime.now(timezone.utc)

    while True:
        now = datetime.now(timezone.utc)

        # 오래된 데이터 제거
        while shared_window and (now - shared_window[0][0]) > timedelta(seconds=WINDOW_SECONDS):
            del shared_window[0]

        # 일정 간격마다 시각화 갱신
        if (now - last_update).total_seconds() >= SLIDING_INTERVAL_SECONDS and shared_window:
            word_counter = Counter()
            sentiment_counter = Counter({'positive': 0, 'neutral': 0, 'negative': 0})

            for _, words, sentiment in shared_window:
                word_counter.update(words)
                if sentiment:
                    sentiment_counter[sentiment.lower()] += 1

            # 막대그래프 (단어 카운트)
            ax1.cla()
            top_words = word_counter.most_common(10)
            words = [w for w, _ in top_words]
            counts = [c for _, c in top_words]
            bars = ax1.bar(words, counts, color='skyblue', edgecolor='black')
            ax1.set_title("Top 10 Words (Last 3 Minutes)", fontsize=14, fontweight='bold')
            ax1.set_ylabel("Count", fontsize=12)
            ax1.tick_params(axis='x', rotation=45)
            ax1.grid(axis='y', linestyle='--', alpha=0.6)

            for bar in bars:
                height = bar.get_height()
                ax1.annotate(f'{height}', xy=(bar.get_x() + bar.get_width() / 2, height),
                             xytext=(0, 5), textcoords="offset points", ha='center', fontsize=10)

            # 파이차트 (감정 분석)
            ax2.cla()
            labels = []
            sizes = []
            colors = {'positive': 'lightblue', 'neutral': 'lightgray', 'negative': 'salmon'}
            pie_colors = []

            for k in ['positive', 'neutral', 'negative']:
                if sentiment_counter[k] > 0:
                    labels.append(k.capitalize())
                    sizes.append(sentiment_counter[k])
                    pie_colors.append(colors[k])

            if sizes:
                wedges, texts, autotexts = ax2.pie(
                    sizes,
                    labels=labels,
                    autopct='%1.1f%%',
                    startangle=140,
                    colors=pie_colors,
                    textprops={'fontsize': 10}
                )
            ax2.set_title("Sentiment Distribution", fontsize=14, fontweight='bold') 

            plt.tight_layout()
            plt.pause(0.01)
            last_update = now

        time.sleep(0.5)

# 메인 함수
if __name__ == "__main__":
    print("📡 Starting Consumer and Visualization...")
    with Manager() as manager:
        shared_window = manager.list()
        consumer_proc = Process(target=consume_data, args=(shared_window,))
        consumer_proc.start()
        run_visualization(shared_window)
