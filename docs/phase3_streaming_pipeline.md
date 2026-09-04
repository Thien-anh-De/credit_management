# ⚡ Giai đoạn 3: Giả lập Quẹt Thẻ 24/7 & Hứng Luồng Real-Time (Phase 3)

**Mục tiêu:** Xây dựng luồng truyền nhận và xử lý dữ liệu giao dịch quẹt thẻ thời gian thực liên tục 24/7.

---

## 🛠️ Chi tiết Công việc & Kiến trúc Streaming:

### 1. Máy phát Luồng Quẹt Thẻ POS (`src/streaming_jobs/kafka_producer.py`)
- **Vai trò:** Giả lập hàng nghìn máy quẹt thẻ tín dụng tại các cửa hàng ngoài đời thực.
- **Cách hoạt động:**
  - Đọc tập giao dịch từ file CSV thô `data/bronze/credit_card_transactions-ibm_v2.csv`.
  - Gắn mốc thời gian thực tại của hệ thống (`datetime.now()`) vào từng giao dịch.
  - Đóng gói dữ liệu dạng JSON và liên tục đẩy vào Kafka Topic `transactions_topic`.
  - Đặt trong vòng lặp vô hạn `while True` với cơ chế **Tự động kết nối lại Kafka (10 lần thử)** giúp hệ thống chạy 24/7 không bao giờ dừng.

### 2. Bộ não Hứng Luồng Spark Streaming (`src/streaming_jobs/spark_consumer.py`)
- **Vai trò:** Lắng nghe, làm sạch và lưu trữ dữ liệu thời gian thực.
- **Cách hoạt động:**
  - Đọc luồng dữ liệu từ Kafka Topic `transactions_topic` bằng **Spark Structured Streaming**.
  - Bóc tách chuỗi JSON thành Schema định dạng rõ ràng (User ID, Card Index, Amount, Merchant, Is Fraud).
  - Làm sạch cột số tiền `amount` (xóa dấu `$`), chuẩn hóa cờ gian lận `is_fraud`.
  - Thực hiện ghi dữ liệu dạng **Append Mode (Ghi nối tiếp)** theo từng **Micro-batch 5 giây** vào tầng Silver: `data/silver/fact_transactions/`.
  - Cấu hình **Checkpointing (`data/silver/checkpoints/`)** để lưu vết Offset Kafka, đảm bảo không mất dữ liệu và không ghi trùng (Exactly-Once Semantics).

---

## 💡 Kết quả đạt được:
Luồng dữ liệu giao dịch quẹt thẻ chảy liên tục 24/7, nạp hàng chục nghìn giao dịch thời gian thực vào Data Lake một cách mượt mà và tin cậy.
