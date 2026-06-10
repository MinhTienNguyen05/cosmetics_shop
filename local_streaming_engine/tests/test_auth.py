import os
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

# Đọc biến môi trường
load_dotenv(dotenv_path="../.env")

try:
    # Khởi tạo Client
    w = WorkspaceClient(
        host=os.environ.get("DATABRICKS_HOST"),
        token=os.environ.get("DATABRICKS_TOKEN")
    )

    # Gọi thử một hàm cơ bản: Lấy danh sách các Cluster đang chạy
    clusters = w.clusters.list()
    print("✅ XÁC THỰC THÀNH CÔNG! Token của bạn hoàn toàn hợp lệ.")
    print(f"Đã tìm thấy {len(list(clusters))} cluster(s) trong Workspace.")

except Exception as e:
    print(f"❌ XÁC THỰC THẤT BẠI: {e}")