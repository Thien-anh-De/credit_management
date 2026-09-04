# 🚀 Giai đoạn 1: Khởi tạo Hạ tầng & Containerization (Phase 1)

**Mục tiêu:** Khởi tạo môi trường phát triển, cấu trúc dự án và đóng gói hệ thống hạ tầng bằng Docker Compose.

---

## 🛠️ Các bước đã thực hiện:

### 1. Chuẩn bị Thư mục & Dữ liệu gốc (Bronze Zone)
- Khởi tạo cấu trúc thư mục tiêu chuẩn: `data/bronze`, `data/silver`, `src/batch_jobs`, `src/streaming_jobs`, `docs`.
- Đưa 3 tệp CSV thô từ IBM Credit Card Dataset vào thư mục `data/bronze/`:
  - `sd254_users.csv`: Dữ liệu 2,000 người dùng.
  - `sd254_cards.csv`: Dữ liệu 6,146 thẻ tín dụng.
  - `credit_card_transactions-ibm_v2.csv`: Tập giao dịch quẹt thẻ lớn.

### 2. Thiết lập Containerization (`docker-compose.yml`)
Dựng toàn bộ dịch vụ cần thiết chạy độc lập trong Docker Containers:
- **Zookeeper (`zookeeper:2181`)**: Quản lý trạng thái cụm Kafka.
- **Kafka Broker (`kafka:9092`)**: Bộ trung chuyển tin nhắn giao dịch thời gian thực (`transactions_topic`).
- **Spark Cluster (`spark-master:7077` & `spark-worker`)**: Bộ não phân tán xử lý tính toán Big Data.
- **Spark Streaming Consumer (`spark-streaming-consumer`)**: Tiến trình chạy ngầm 24/7 đọc luồng Kafka và ghi Parquet.
- **Web Dashboard (`web_dashboard`)**: Trạm giám sát thời gian thực Streamlit chạy tại cổng `http://localhost:8501`.

---

## 💡 Kết quả đạt được:
Hệ thống hạ tầng khởi chạy đồng bộ 100% chỉ với lệnh:
```bash
docker compose up -d
```
