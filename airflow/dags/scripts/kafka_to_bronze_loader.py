import json
import os
import time
from kafka import KafkaConsumer
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv(dotenv_path="/opt/airflow/.env")

w = WorkspaceClient(
    host=os.environ.get("DATABRICKS_HOST"),
    token=os.environ.get("DATABRICKS_TOKEN")
)

VOLUME_INBOX_PATH = "/Volumes/workspace/bronze_cosmetics/inbox"
BATCH_SIZE = 1000
MAX_BATCHES_PER_RUN = 5  # Số batch tối đa mỗi lần chạy để đảm bảo task không chạy quá lâu
batch_data = []
batches_processed = 0

# 2. Khởi tạo Kafka Consumer
# consumer_timeout_ms cực kỳ quan trọng: nó sẽ khiến vòng lặp for tự dừng
# khi không có message mới nào trong 10 giây
consumer = KafkaConsumer(
    'ecommerce_events',
    bootstrap_servers=['kafka-1:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='airflow-ingestion-group',
    request_timeout_ms=15000,
    session_timeout_ms=10000,
    consumer_timeout_ms=10000
)

print("Kết nối thành công! Đang xử lý dữ liệu...")

try:
    for message in consumer:
        batch_data.append(message.value)

        if len(batch_data) >= BATCH_SIZE:
            filename = f"events_batch_{int(time.time())}.json"
            local_filepath = f"/tmp/{filename}" # Ghi vào /tmp để đảm bảo quyền

            with open(local_filepath, 'w') as f:
                for item in batch_data:
                    f.write(json.dumps(item) + '\n')

            remote_filepath = f"{VOLUME_INBOX_PATH}/{filename}"
            print(f"Đang đẩy {filename} lên Databricks...")

            with open(local_filepath, 'rb') as f:
                w.files.upload(remote_filepath, f)

            os.remove(local_filepath)
            batch_data = []
            batches_processed += 1
            print(f"Đẩy thành công batch thứ {batches_processed}!")

            # DỪNG LẠI SAU KHI ĐẠT GIỚI HẠN MỖI LẦN CHẠY
            if batches_processed >= MAX_BATCHES_PER_RUN:
                print("Đã đạt giới hạn batch, kết thúc nhiệm vụ.")
                break
except Exception as e:
    print(f"Lỗi xảy ra: {e}")
finally:
    consumer.close()
    print("Đóng kết nối Kafka, script kết thúc.")