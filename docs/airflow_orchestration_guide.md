# 🌬️ HƯỚNG DẪN ĐIỀU PHỐI KHO DỮ LIỆU BẰNG APACHE AIRFLOW
## (Gold Layer DWH Orchestration & Medallion Stream-Batch Separation)

---

## 📌 1. Kiến Trúc Tách Biệt Stream & Batch (Stream-Batch Separation)

Hệ thống tuân thủ mô hình kiến trúc Hồ dữ liệu hiện đại (**Medallion Lakehouse**):
* **Tầng Stream (Bronze ➔ Silver):** Hoạt động liên tục **24/7** qua Apache Kafka (`kafka_producer.py`) và Spark Structured Streaming (`spark_consumer.py`) để tiếp nhận giao dịch POS thời gian thực, làm sạch và phân tách lỗi vào vùng cách ly Quarantine.
* **Tầng Batch Orchestration (Silver ➔ Gold):** Được giao phó cho **Apache Airflow**. Định kỳ hàng ngày (ví dụ 02:00 AM sau giờ chốt sổ ngân hàng), Airflow kích hoạt DAG để quét toàn bộ dữ liệu Silver đã tích lũy, xây dựng mô hình **Kimball Star Schema**, tính toán **SCD Type 2 Point-in-Time Join**, cuộn tháp tổng hợp **Inmon Roll-up** và chạy chốt đối soát tài chính **Quality Gate**.

* **DAG ID:** `credit_management_gold_dwh`
* **File định nghĩa:** [`dags/credit_management_dwh_dag.py`](file:///e:/credit_management/dags/credit_management_dwh_dag.py)
* **Lịch trình:** `0 2 * * *` (Hàng ngày lúc 02:00 AM)

---

## 🏗️ 2. Đồ thị Phụ thuộc Tác vụ Tầng Gold (Task Dependency Graph)

```text
[TIỀN ĐỀ: Dữ liệu Bronze & Silver được Streaming 24/7 qua Kafka]
                               │
                      [ start_gold_pipeline ]
                               │
      ┌────────────────┬───────┴────────┬────────────────┐
      ▼                ▼                ▼                ▼
[ build_dim_date ] [ build_dim_merch ] [ build_dim_users ] [ build_dim_cards ]
  (SCD Type 0)      (SCD Type 1)        (SCD Type 2)        (SCD Type 2)
      └────────────────┬────────────────┴────────────────┘
                       ▼ (4 Dimensions hội tụ)
      [ build_fact_transactions_pit_join_gold ]
       (SCD2 Point-in-Time Join & Atomic Grain)
                       │
                       ▼
         [ build_inmon_summary_layers ]
          (Tháp Rollup Ngày ➔ Tháng ➔ Quý)
                       │
                       ▼
       [ data_quality_and_benchmark_gate ]
        (0 Orphan FK, 100% Unique, Đối soát Audit)
                       │
                       ▼
               [ end_gold_pipeline ]
```

### Chi tiết các Task trong Gold DAG:
| STT | Task ID | Vai trò & Tác vụ thực hiện | Loại SCD / Chức năng |
|:---:|:---|:---|:---:|
| 1 | `start_gold_pipeline` | Điểm bắt đầu chu kỳ nạp Gold | Orchestration |
| 2 | `build_dim_date_gold` | Sinh lịch tĩnh 14,975 ngày từ 1995–2035 | SCD Type 0 (Fixed) |
| 3 | `build_dim_merchant_gold` | Trích xuất Merchant từ Silver, sinh `merchant_sk` | SCD Type 1 (Overwrite) |
| 4 | `build_dim_users_scd2_gold` | Xây dựng phiên bản khách hàng (`effective_date`, FICO) | SCD Type 2 (Versioning) |
| 5 | `build_dim_cards_scd2_gold` | Xây dựng phiên bản thẻ (hạn mức, cảnh báo Dark Web) | SCD Type 2 (Versioning) |
| 6 | `build_fact_transactions_pit_join_gold` | Ghép nối **Point-in-Time Join** kẹp thời gian gắn đúng `card_sk`, `user_sk` | Fact Grain & Audit |
| 7 | `build_inmon_summary_layers` | Xây dựng tháp Inmon Rollup (Ngày ➔ Tháng ➔ Quý & Card Fraud) | Data Marts / OLAP |
| 8 | `data_quality_and_benchmark_gate` | Chốt kiểm định: 0 orphan FK, 100% Unique, đối soát $1,622,991.69 | Quality Gate & Audit |
| 9 | `end_gold_pipeline` | Hoàn tất chu kỳ nạp Gold an toàn | Orchestration |

---

## 🚀 3. Hướng dẫn Vận hành

### Cách 1: Kiểm tra nhanh cấu trúc DAG (Terminal - Không cần Docker)
```powershell
python src/tools/test_airflow_dag.py
```

### Cách 2: Khởi động Web UI Airflow (Docker)
```powershell
.\start_airflow.ps1
```
* **Web UI Airflow:** [http://localhost:8085](http://localhost:8085)
* **Tài khoản:** `admin` / `admin`
* Bật hoặc bấm nút **Trigger DAG** `credit_management_gold_dwh` để chạy chu kỳ nạp Gold.
