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
MAX_BATCHES_PER_RUN = 250

# Hàm tiện ích: Ghi file và đẩy lên Databricks
def flush_and_upload(data_list):
    if not data_list:
        return False

    # Thêm timestamp mili-giây để tên file luôn unique, không bị ghi đè
    filename = f"events_batch_{int(time.time()*1000)}.json"
    local_filepath = f"/tmp/{filename}"

    # 1. Ghi ra file
    with open(local_filepath, 'w') as f:
        for item in data_list:
            f.write(json.dumps(item) + '\n')

    remote_filepath = f"{VOLUME_INBOX_PATH}/{filename}"
    print(f"Đang đẩy file {filename} ({len(data_list)} events) lên Databricks...")

    # 2. Tải lên Databricks
    with open(local_filepath, 'rb') as f:
        w.files.upload(remote_filepath, f)

    # 3. Dọn rác
    os.remove(local_filepath)
    return True


# Cấu hình Kafka Consumer CHỐNG RỚT DATA
consumer = KafkaConsumer(
    'ecommerce_events',
    bootstrap_servers=['kafka-1:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='airflow-ingestion-group',
    enable_auto_commit=False,  # BẮT BUỘC TẮT: Để tự kiểm soát việc xác nhận đọc thành công
    request_timeout_ms=15000,
    session_timeout_ms=10000,
    consumer_timeout_ms=10000
)

print("Kết nối thành công! Đang xử lý dữ liệu...")
batch_data = []
batches_processed = 0

try:
    for message in consumer:
        batch_data.append(message.value)

        # Khi giỏ hàng đủ 1000 kiện
        if len(batch_data) >= BATCH_SIZE:
            # Nếu hàm upload Databricks thành công 100%
            if flush_and_upload(batch_data):
                # Lúc này mới BÁO CÁO VỚI KAFKA là đã đọc an toàn
                consumer.commit()
                batches_processed += 1
                print(f"Đẩy thành công batch thứ {batches_processed}!")

            # Làm trống giỏ hàng cho chuyến tiếp theo
            batch_data = []

            # Kiểm tra định mức
            if batches_processed >= MAX_BATCHES_PER_RUN:
                print("Đã đạt giới hạn batch, tạm dừng nhiệm vụ để nhường tài nguyên.")
                break

    # FIX LỖI RỚT DATA: Vòng lặp thoát nhưng vẫn còn dữ liệu dở dang (ví dụ 450 dòng)
    if len(batch_data) > 0:
        print("Phát hiện dữ liệu tồn đọng cuối cùng, đang xả nốt...")
        if flush_and_upload(batch_data):
            consumer.commit()
            print("Đã xả thành công phần dữ liệu đuôi.")

except Exception as e:
    print(f"Lỗi xảy ra trong quá trình xử lý: {e}")
finally:
    consumer.close()
    print("Đóng kết nối Kafka, script kết thúc trọn vẹn.")