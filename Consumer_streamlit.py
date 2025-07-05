import streamlit as st
import matplotlib.pyplot as plt
import boto3
import json
import time
from collections import deque, Counter
from datetime import datetime, timedelta, timezone
import threading

# ✅ AWS Kinesis 설정
REGION_NAME = "us-east-1"
STREAM_NAME = "book-reviews-stream"

# ✅ 슬라이딩 윈도우 설정 (3분 유지, 5초마다 갱신)
WINDOW_SECONDS = 180
SLIDING_INTERVAL_SECONDS = 5

# ✅ 상태 저장용
shared_window = deque()

# ✅ 텍스트 전처리 함수
def tokenize(text):
    if not isinstance(text, str):
        return []
    return text.lower().split()

# ✅ Kinesis 소비 스레드
def consume_data():
    kinesis = boto3.client("kinesis", region_name=REGION_NAME)
    shards = kinesis.describe_stream(StreamName=STREAM_NAME)["StreamDescription"]["Shards"]
    shard_iterators = []

    for shard in shards:
        try:
            iterator = kinesis.get_shard_iterator(
                StreamName=STREAM_NAME,
                ShardId=shard["ShardId"],
                ShardIteratorType="LATEST"
            )["ShardIterator"]
            shard_iterators.append((shard["ShardId"], iterator))
        except Exception as e:
            print(f"❌ Shard iterator error: {e}")

    while True:
        for idx, (shard_id, iterator) in enumerate(shard_iterators):
            try:
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
            except:
                continue
        time.sleep(0.5)

# ✅ Streamlit 설정
st.set_page_config(layout="wide")
st.title("📡 Real-time Amazon Book Review Dashboard")

# ✅ UI 공간 확보
placeholder = st.empty()

# ✅ 소비 스레드 실행
threading.Thread(target=consume_data, daemon=True).start()

# ✅ 실시간 시각화 루프
while True:
    now = datetime.now(timezone.utc)

    # 오래된 항목 제거
    while shared_window and (now - shared_window[0][0]) > timedelta(seconds=WINDOW_SECONDS):
        shared_window.popleft()

    # 스냅샷 복사 후 분석
    snapshot = list(shared_window)
    if snapshot:
        start_time = time.time()
        
        word_counter = Counter()
        sentiment_counter = Counter({'positive': 0, 'neutral': 0, 'negative': 0})
        for _, words, sentiment in snapshot:
            word_counter.update(words)
            if sentiment:
                sentiment_counter[sentiment.lower()] += 1
        
        end_time = time.time()
        elapsed = end_time - start_time
        throughput = len(snapshot) / elapsed if elapsed > 0 else 0
        latency = (elapsed / max(len(snapshot), 1)) * 1000  # ms

        # Top Words 시각화
        top_words = word_counter.most_common(10)
        words, counts = zip(*top_words) if top_words else ([], [])

        fig1, ax1 = plt.subplots(figsize=(6, 4))
        bars = ax1.bar(words, counts, color='#87CEFA', edgecolor='black')
        ax1.set_title("Top 10 Words (Last 3 Minutes)", fontsize=14, fontweight='bold')
        ax1.set_ylabel("Count", fontsize=12)
        ax1.tick_params(axis='x', labelrotation=45)
        ax1.grid(axis='y', linestyle='--', alpha=0.5)
        for i, count in enumerate(counts):
            ax1.text(i, count + max(counts) * 0.01, str(count), ha='center', fontsize=9)

        # Sentiment Pie 시각화
        labels, sizes, pie_colors = [], [], []
        color_map = {'positive': '#A1D6E2', 'neutral': '#CCCCCC', 'negative': '#FF9999'}

        for s in ['positive', 'neutral', 'negative']:
            if sentiment_counter[s] > 0:
                labels.append(s.capitalize())
                sizes.append(sentiment_counter[s])
                pie_colors.append(color_map[s])

        fig2, ax2 = plt.subplots(figsize=(5, 4))
        if sizes:
            wedges, texts, autotexts = ax2.pie(
                sizes,
                labels=labels,
                autopct='%1.1f%%',
                startangle=140,
                colors=pie_colors,
                textprops={'fontsize': 10}
            )
            for text in texts:
                text.set_fontweight('bold')
        ax2.set_title("Sentiment Distribution", fontsize=14, fontweight='bold')

        # Streamlit 표시
        with placeholder.container():
            
            # 실시간 성능 지표
            st.subheader("🔧 Stream Processing Performance")
            col_perf1, col_perf2, col_perf3 = st.columns(3)
            col_perf1.metric("Throughput", f"{throughput:.2f}", "records/sec")
            col_perf2.metric("Latency", f"{latency:.4f}", "ms/record")
            col_perf3.metric("Snapshot Size", f"{len(snapshot)}")
            
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Top 10 Words (Last 3 Minutes)")
                st.pyplot(fig1)
                plt.close(fig1)
            with col2:
                st.subheader("Sentiment Distribution")
                st.pyplot(fig2)
                plt.close(fig2)

    time.sleep(SLIDING_INTERVAL_SECONDS)
