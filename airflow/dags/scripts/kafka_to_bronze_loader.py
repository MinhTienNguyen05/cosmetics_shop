import json
import os
import time
from kafka import KafkaConsumer
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/.env")

w = WorkspaceClient(
    host=os.environ.get("DATABRICKS_HOST"),
    token=os.environ.get("DATABRICKS_TOKEN"),
)

VOLUME_INBOX_PATH = "/Volumes/workspace/bronze_cosmetics/inbox"
BATCH_SIZE = 1000
MAX_BATCHES_PER_RUN = 250


def is_valid_event(ev):
    """Defense-in-depth: chặn row thiếu khoá chính trước khi đẩy lên Volume."""
    return isinstance(ev, dict) and bool(ev.get("user_id")) and bool(ev.get("product_id"))


def flush_and_upload(data_list):
    if not data_list:
        return False

    filename = f"events_batch_{int(time.time() * 1000)}.json"
    local_filepath = f"/tmp/{filename}"

    with open(local_filepath, "w") as f:
        for item in data_list:
            f.write(json.dumps(item) + "\n")

    remote_filepath = f"{VOLUME_INBOX_PATH}/{filename}"
    print(f"Đang đẩy file {filename} ({len(data_list)} events) lên Databricks...")

    with open(local_filepath, "rb") as f:
        w.files.upload(remote_filepath, f)

    os.remove(local_filepath)
    return True


# Kafka Consumer: tắt auto-commit, chỉ commit sau khi upload thành công.
consumer = KafkaConsumer(
    "ecommerce_events",
    bootstrap_servers=["kafka-1:9092"],
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="airflow-ingestion-group",
    enable_auto_commit=False,
    request_timeout_ms=15000,
    session_timeout_ms=10000,
    consumer_timeout_ms=10000,
)

print("Kết nối thành công! Đang xử lý dữ liệu...")
batch_data = []
batches_processed = 0
skipped_invalid = 0

try:
    for message in consumer:
        ev = message.value
        if not is_valid_event(ev):
            skipped_invalid += 1
            continue

        batch_data.append(ev)

        if len(batch_data) >= BATCH_SIZE:
            if flush_and_upload(batch_data):
                consumer.commit()
                batches_processed += 1
                print(f"Đẩy thành công batch thứ {batches_processed}!")
            batch_data = []

            if batches_processed >= MAX_BATCHES_PER_RUN:
                print("Đã đạt giới hạn batch, tạm dừng để nhường tài nguyên.")
                break

    if len(batch_data) > 0:
        print("Phát hiện dữ liệu tồn đọng cuối cùng, đang xả nốt...")
        if flush_and_upload(batch_data):
            consumer.commit()
            print("Đã xả thành công phần dữ liệu đuôi.")

except Exception as e:
    print(f"Lỗi xảy ra trong quá trình xử lý: {e}")
finally:
    consumer.close()
    print(f"Script kết thúc. batches={batches_processed}, skipped_invalid={skipped_invalid}")
