# 🏗️ Kiến trúc Hệ thống & Cấu trúc Thư mục (Architecture & Repository)

Hệ thống được thiết kế theo mô hình **Real-Time Data Lakehouse Architecture** tinh gọn, tối ưu hóa năng lực xử lý phân tán của **Apache Spark** và bộ đệm tin nhắn **Apache Kafka**.

---

## 1. Sơ đồ Kiến trúc Luồng Dữ liệu (Data Flow Architecture)

```text
[ Dữ liệu thô CSV (Bronze Zone) ]
       │
       ├────► [ Process Users & Cards (Batch) ] ────────────► [ Silver Data Lake: dim_users & dim_cards (Parquet) ]
       │                                                                      │
       └────► [ Kafka Producer (Quẹt thẻ 24/7) ]                              │
                     │                                                        │
                     ▼                                                        │
              [ Kafka Broker ]                                                │
                     │                                                        │
                     ▼                                                        │
          [ Spark Streaming Consumer ] ─────────────────────► [ Silver Data Lake: fact_transactions (Parquet) ]
                                                                              │
                                                                              ▼
                                                                [ Streamlit Web Dashboard (Port 8501) ]
                                                                [ Jupyter Notebook (Analytics & EDA) ]
```

### Các thành phần chính:
* **Bronze Zone (Data Source):** Lưu trữ 3 tệp CSV thô ban đầu (`users`, `cards`, `transactions`).
* **Ingestion & Messaging Layer:** 
  * **Batch:** Xử lý dữ liệu định danh người dùng và thẻ.
  * **Streaming:** `kafka_producer.py` giả lập hàng nghìn máy POS bắn tin nhắn quẹt thẻ thời gian thực 24/7 vào Kafka topic `transactions_topic`.
* **Processing Engine (Apache Spark 3.5.1):**
  * **Batch Engine:** Làm sạch, nạp dữ liệu tĩnh vào tầng Silver (`dim_users`, `dim_cards`).
  * **Structured Streaming Engine:** Hứng luồng Kafka mỗi 5 giây (Micro-batch), bóc tách JSON, làm sạch và ghi nối tiếp vào `fact_transactions`.
* **Silver Zone (Data Lake):** Lưu trữ dữ liệu chuẩn Parquet nén Snappy tối ưu hóa dung lượng và tốc độ truy vấn.
* **Visualization & Analytics Layer:**
  * **Web Operations Center (`web_dashboard.py`):** Giao diện Web tương tác bằng Streamlit & Plotly hiển thị 5 thẻ KPI, biểu đồ tốc độ nạp, biểu đồ loại thẻ và cảnh báo gian lận thời gian thực.
  * **Jupyter Notebook (`notebooks/data_analysis.ipynb`):** Dùng PySpark truy vấn và phân tích EDA sâu.

---

## 2. Cấu trúc Thư mục Repository

```text
credit_managerment/
├── data/
│   ├── bronze/                  # Chứa 3 file CSV thô ban đầu (Data gốc)
│   └── silver/                  # Tầng Silver Data Lake (Định dạng Parquet)
│       ├── dim_users/           # Bảng chiều 2,000 Khách hàng
│       ├── dim_cards/           # Bảng chiều 6,146 Thẻ tín dụng
│       ├── fact_transactions/   # Bảng sự kiện giao dịch Streaming 24/7
│       └── checkpoints/         # Thư mục lưu vết Offset của Spark Streaming
├── docs/                        # Tài liệu hướng dẫn chi tiết từng giai đoạn (Phase 1-4)
├── src/
│   ├── batch_jobs/              # Mã nguồn PySpark xử lý dữ liệu lô (Batch)
│   │   ├── process_users.py
│   │   └── process_cards.py
│   ├── streaming_jobs/          # Mã nguồn Kafka & Spark Streaming (Real-Time)
│   │   ├── kafka_producer.py
│   │   └── spark_consumer.py
│   └── web_dashboard.py         # Ứng dụng Web Dashboard Giám sát 24/7 (Streamlit)
├── notebooks/                   # Jupyter Notebook dùng để phân tích EDA sâu
│   └── data_analysis.ipynb
├── docker-compose.yml           # Khởi chạy Zookeeper, Kafka, Spark Cluster, Dashboard
└── requirements.txt             # Danh sách thư viện Python phụ thuộc
```
