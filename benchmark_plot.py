import boto3
import pandas as pd
import matplotlib.pyplot as plt

# 📥 S3에서 CSV 다운로드
s3 = boto3.client("s3")
s3.download_file("bookreview-results", "hybrid/benchmark_metrics_sequential.csv", "metrics_seq.csv")
s3.download_file("bookreview-results", "hybrid/benchmark_metrics_parallel.csv", "metrics_par.csv")

# 📊 CSV 데이터 로드
seq_df = pd.read_csv("metrics_seq.csv")
par_df = pd.read_csv("metrics_par.csv")

# ⚙️ 설정
tasks = ["wordcount", "sentiment"]
metrics = ["throughput_rps", "latency_spr", "time_sec"]
titles = {
    "throughput_rps": "Throughput (records/sec)",
    "latency_spr": "Latency (ms/record)",
    "time_sec": "Processing Time (sec)"
}
bar_width = 0.35
loads = [25, 50, 75, 100]
x = list(range(len(loads)))

# 🎨 지표별 색상 설정
colors = {
    "throughput_rps": ("plum", "lightskyblue"),
    "latency_spr": ("skyblue", "salmon"),
    "time_sec": ("lightgreen", "tomato")
}

# 📈 그래프 생성
for metric in metrics:
    for task in tasks:
        fig, ax = plt.subplots(figsize=(8, 5))

        # 데이터 필터링
        seq_y = seq_df[seq_df["task"] == task][metric].tolist()
        par_y = par_df[par_df["task"] == task][metric].tolist()

        # Latency는 ms 단위로 변환
        if metric == "latency_spr":
            seq_y = [val * 1000 for val in seq_y]
            par_y = [val * 1000 for val in par_y]

        # 색상 적용
        seq_color, par_color = colors[metric]

        # 막대그래프 그리기
        ax.bar([xi - bar_width/2 for xi in x], seq_y, width=bar_width, label="Sequential", color=seq_color)
        ax.bar([xi + bar_width/2 for xi in x], par_y, width=bar_width, label="Parallel", color=par_color)

        # 막대 위에 수치 표시
        for xi, yi in zip([xi - bar_width/2 for xi in x], seq_y):
            ax.text(xi, yi, f"{yi:.3f}", ha='center', va='bottom', fontsize=8)
        for xi, yi in zip([xi + bar_width/2 for xi in x], par_y):
            ax.text(xi, yi, f"{yi:.3f}", ha='center', va='bottom', fontsize=8)

        # 그래프 설정
        ax.set_title(f"{titles[metric]} - {task.capitalize()}")
        ax.set_xlabel("Load (%)")
        ax.set_ylabel("Value")
        ax.set_xticks(x)
        ax.set_xticklabels([str(l) for l in loads])
        ax.legend()
        ax.grid(True, linestyle="--", alpha=0.5)
        fig.tight_layout()

        # 저장 및 업로드
        filename = f"{metric}_{task}_comparison.png"
        fig.savefig(filename)
        print(f"✅ Saved {filename}")

        try:
            s3.upload_file(filename, "bookreview-results", f"hybrid/{filename}")
            print(f"✅ Uploaded {filename} to S3")
        except Exception as e:
            print(f"❌ Upload failed: {e}")