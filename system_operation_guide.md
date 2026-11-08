# 🗺️ Hướng Dẫn Vận Hành Hệ Thống credit_management
## Đọc Hiểu — Khởi Động — Theo Dõi — Phát Triển

---

## 1. Bức Tranh Toàn Cảnh — Hệ Thống Làm Gì?

Hệ thống mô phỏng **ngân hàng xử lý giao dịch thẻ tín dụng 24/7**, gồm 3 tầng dữ liệu chạy song song:

```
                    ┌─────────────────────────────────┐
                    │  NGUỒN GỐC (Bronze)             │
                    │  data/bronze/*.csv              │
                    │  (3 file CSV từ IBM Dataset)    │
                    └──────────┬──────────────────────┘
                               │
              ┌────────────────┴────────────────┐
              ▼                                 ▼
  ┌─────────────────────┐         ┌─────────────────────────┐
  │  BATCH PIPELINE      │         │  STREAMING PIPELINE      │
  │  process_users.py    │         │  kafka_producer.py       │
  │  process_cards.py    │         │    (giả lập máy POS)     │
  │  (chạy 1 lần, ~3s)  │         │         ↓                │
  └──────────┬──────────┘         │  Apache Kafka Broker     │
             │                    │  (topic: transactions)   │
             │                    │         ↓                │
             │                    │  spark_consumer.py       │
             │                    │  (micro-batch 5 giây)    │
             └──────────┬─────────┘
                        ▼
         ┌──────────────────────────┐
         │  SILVER LAYER            │
         │  data/silver/            │
         │  ├── dim_users/          │  ← 2,000 user profiles
         │  ├── dim_cards/          │  ← 6,146 thẻ tín dụng
         │  └── fact_transactions/  │  ← liên tục tăng (append)
         └──────────────┬───────────┘
                        │
                        │  [Chạy thủ công sau khi Silver có đủ data]
                        ▼
         ┌──────────────────────────────────────┐
         │  GOLD LAYER / DWH (Kimball)          │
         │  data/gold/                          │
         │  ├── dim_date/        (7,670 rows)   │
         │  ├── dim_merchant/                   │
         │  ├── dim_users/  + SCD Type 2        │
         │  ├── dim_cards/  + SCD Type 2        │
         │  ├── fact_transactions/ (Surrogate K)│
         │  ├── agg_daily_card_fraud/           │
         │  ├── agg_monthly_merchant_vol/       │
         │  └── agg_quarterly_merchant_vol/     │
         └──────────────┬───────────────────────┘
                        │
              ┌─────────┴──────────┐
              ▼                    ▼
  ┌────────────────────┐  ┌────────────────────────┐
  │  WEB DASHBOARD     │  │  KIỂM THỬ DWH          │
  │  web_dashboard.py  │  │  07_dq_and_benchmark.py│
  │  :8501 (Streamlit) │  │  → data_quality_rep│
  └────────────────────┘  └────────────────────────┘
```

---

## 2. Điều Kiện Tiên Quyết

### Phần mềm cần cài đặt:
- **Docker Desktop** (bật WSL2 backend trên Windows)
- **Python 3.10+** — cho script DWH chạy local
- **PySpark** — `pip install pyspark` (hoặc dùng spark-submit bên trong Docker)

### Kiểm tra Docker đang chạy:
```powershell
docker --version        # Docker version 24.x+
docker compose version  # Docker Compose version 2.x+
```

### Kiểm tra cổng còn trống:
```powershell
# Các cổng hệ thống cần dùng:
netstat -ano | findstr "8080 8081 8501 9092 2181 7077"
# Nếu có kết quả → cổng đang bị chiếm → cần kill process đó trước
```

---

## 3. Chuẩn Bị Dữ Liệu Bronze (Làm 1 Lần Duy Nhất)

Bronze là nơi chứa file CSV gốc — **không được tự động sinh**, phải tải về thủ công.

```
data/bronze/
├── sd254_users.csv                          ← 2,000 users
├── sd254_cards.csv                          ← 6,146 cards
└── credit_card_transactions-ibm_v2.csv      ← hàng trăm nghìn giao dịch
```

**Link tải:** https://drive.google.com/drive/folders/1vymvt8bZYZYNWhMuC_XnDR8N7PXDVoMc

Sau khi tải, đặt đúng đường dẫn trên. Kiểm tra:
```powershell
ls e:\credit_management\data\bronze\
# Phải thấy 3 file CSV
```

---

## 4. Khởi Động Hệ Thống (Docker Pipeline)

> [!IMPORTANT]
> Phải có đủ 3 file CSV ở `data/bronze/` trước khi chạy bước này.

### Lệnh khởi động:
```powershell
cd e:\credit_management
docker compose up -d
```

### Thứ tự tự động khởi động của Docker (quan trọng để hiểu):

```
t=0s   → Zookeeper khởi động (quản lý Kafka cluster)
t=5s   → Kafka Broker khởi động, tạo topic "transactions_topic"
t=10s  → Spark Master + Worker sẵn sàng
t=15s  → spark-batch-users  : chạy process_users.py → tạo data/silver/dim_users/
t=15s  → spark-batch-cards  : chạy process_cards.py → tạo data/silver/dim_cards/
t=20s  → spark-streaming-consumer : kết nối Kafka, lắng nghe topic
t=25s  → kafka-producer     : bắt đầu bắn giao dịch vào Kafka (mỗi 0.05s = 20 msg/s)
t=30s  → web-dashboard      : Streamlit Dashboard lên tại http://localhost:8501
```

### Kiểm tra tất cả container đang chạy:
```powershell
docker compose ps
```
**Output bình thường:**
```
NAME                      STATUS
zookeeper                 Up
kafka                     Up
spark-master              Up
spark-worker              Up
spark-streaming-consumer  Up
kafka-producer            Up
web-dashboard             Up
```

> [!NOTE]
> `spark-batch-users` và `spark-batch-cards` có trạng thái **Exited (0)** là **BÌNH THƯỜNG** — chúng chạy xong 1 lần rồi thoát, không phải lỗi.

---

## 5. Xem Log Theo Dõi Thời Gian Thực

### Xem luồng dữ liệu đang chạy:
```powershell
# Xem tất cả log cùng lúc (Ctrl+C để thoát)
docker compose logs -f

# Xem log từng service riêng lẻ:
docker compose logs -f kafka-producer          # Tốc độ bắn giao dịch
docker compose logs -f spark-streaming-consumer # Spark xử lý micro-batch 5s
docker compose logs -f web-dashboard           # Streamlit server
```

### Hiểu log của kafka-producer:
```
Sent batch 100 | Total sent: 1,000 | File offset: 1000/1754155
```
→ Đã bắn 1,000 giao dịch, đang ở dòng 1000/1,754,155 của file CSV.

### Hiểu log của spark-streaming-consumer:
```
Batch: 47 | Records: 234 | Time: 2026-10-22 00:15:30
```
→ Micro-batch thứ 47, xử lý 234 records trong 5 giây vừa qua.

---

## 6. Theo Dõi Qua Giao Diện Web

| URL | Tên | Mục đích |
|-----|-----|----------|
| http://localhost:8501 | **Operations Dashboard** | Giám sát giao dịch thời gian thực, cảnh báo fraud |
| http://localhost:8080 | **Spark Master UI** | Xem cluster, các job đang chạy, tài nguyên CPU/RAM |
| http://localhost:8081 | **Spark Worker UI** | Chi tiết worker, executor |

### Dashboard (8501) hiển thị gì:
- **KPI cards**: Tổng user, thẻ, giao dịch (tự động tăng), tổng tiền, fraud count
- **Line chart**: Throughput theo thời gian (msg/phút)
- **Donut chart**: Tỷ lệ phương thức thanh toán (chip/swipe/online)
- **Live alert table**: Danh sách giao dịch gần nhất, dòng đỏ = gian lận

---

## 7. Kiểm Tra Silver Layer Đã Có Dữ Liệu Chưa

```powershell
# Xem số file Parquet đã được tạo:
ls e:\credit_management\data\silver\fact_transactions\ | Measure-Object

# Đọc thử bằng Python:
python3 -c "
import pandas as pd
df = pd.read_parquet('e:/credit_management/data/silver/fact_transactions/')
print(f'Rows: {len(df):,}')
print(df.dtypes)
print(df.head(3))
"
```

**Schema của fact_transactions Silver** (từ spark_consumer.py):
```
user_id              long
card_index           int
transaction_timestamp timestamp
amount               double
use_chip             string         ← "Chip Transaction" / "Swipe Transaction" / "Online Transaction"
merchant_name        string
merchant_city        string
merchant_state       string
zip                  string
mcc                  string         ← Merchant Category Code (mã ngành)
errors               string
is_fraud             boolean
current_timestamp    string
ingestion_timestamp  string
```

---

## 8. Xây Dựng Gold Layer (DWH) — Chạy Sau Khi Silver Có Đủ Data

> [!IMPORTANT]
> Chạy bước này **SAU KHI** streaming đã chạy đủ thời gian và `data/silver/fact_transactions/` đã có dữ liệu.
> Các DWH job chạy **LOCAL** trên máy bạn, không cần Docker.

### Cách 1: Chạy toàn bộ bằng 1 lệnh (PowerShell):
```powershell
cd e:\credit_management
.\run_dwh_pipeline.ps1
```

### Cách 2: Chạy từng bước (để hiểu từng phần):

```powershell
# BƯỚC 1: Tạo dim_date (7,670 ngày từ 2010-2030)
# Không phụ thuộc gì — chạy trước tiên
spark-submit src/dwh_jobs/01_build_dim_date.py

# BƯỚC 2: Tách Merchant từ Silver fact → Gold dim_merchant
# Phụ thuộc: data/silver/fact_transactions/ phải có data
spark-submit src/dwh_jobs/02_build_dim_merchant.py

# BƯỚC 3: Đẩy dim_users Silver → Gold với SCD Type 2
# Phụ thuộc: data/silver/dim_users/ (tạo từ spark-batch-users)
spark-submit src/dwh_jobs/03_build_dim_users_scd2.py

# BƯỚC 4: Đẩy dim_cards Silver → Gold với SCD Type 2
# Phụ thuộc: data/silver/dim_cards/ (tạo từ spark-batch-cards)
spark-submit src/dwh_jobs/04_build_dim_cards_scd2.py

# BƯỚC 5: Xây dựng fact_transactions Gold (JOIN 4 bảng Dim → Surrogate Keys)
# Phụ thuộc: tất cả 4 bảng Gold Dim phải sẵn sàng
spark-submit src/dwh_jobs/05_build_fact_transactions.py

# BƯỚC 6: Tạo Summary Layers (pre-aggregation)
# Phụ thuộc: data/gold/fact_transactions/ và data/gold/dim_date/
spark-submit src/dwh_jobs/06_build_summary_layers.py
```

### Cách 3: Chạy trong Jupyter Notebook (tiện nhất để demo):
```python
# Cell 1 — setup
import subprocess, sys
def run_job(script):
    print(f"\n{'='*50}\nRunning: {script}\n{'='*50}")
    result = subprocess.run([sys.executable, '-m', 'pyspark', script], capture_output=True, text=True)
    print(result.stdout[-2000:])  # 2000 ký tự cuối log
```

---

## 9. Kiểm Tra Gold Layer Đã Được Tạo Đúng

```powershell
# Xem cấu trúc Gold:
ls e:\credit_management\data\gold\

# Kiểm tra từng bảng:
python3 -c "
import pandas as pd

tables = ['dim_date', 'dim_merchant', 'dim_users', 'dim_cards',
          'fact_transactions', 'agg_daily_card_fraud', 'agg_monthly_merchant_vol']

for t in tables:
    try:
        df = pd.read_parquet(f'e:/credit_management/data/gold/{t}/')
        print(f'✅ {t:<35}: {len(df):>8,} rows | cols: {list(df.columns)}')
    except Exception as e:
        print(f'❌ {t}: {e}')
"
```

**Output mong đợi:**
```
✅ dim_date                         :    7,670 rows | ['date_sk', 'full_date_str', ...]
✅ dim_merchant                     :    X,XXX rows | ['merchant_sk', 'merchant_name', ...]
✅ dim_users                        :    2,000 rows | ['user_sk', 'user_id', ..., 'effective_date', 'expiration_date', 'is_current']
✅ dim_cards                        :    6,146 rows | ['card_sk', 'user_id', ..., 'effective_date', 'expiration_date', 'is_current']
✅ fact_transactions                :  XXX,XXX rows | ['date_sk', 'user_sk', 'card_sk', 'merchant_sk', ...]
✅ agg_daily_card_fraud             :    X,XXX rows | ['date_sk', 'card_sk', 'total_txn', ...]
✅ agg_monthly_merchant_vol         :    X,XXX rows | ['year', 'month_number', 'merchant_sk', ...]
```

---

## 10. Chạy Kiểm Thử DWH (Milestone 5.1)

```powershell
python src/tests/07_dq_and_benchmark.py
```

**Output mẫu:**
```
================================================================
  🔎 A) REFERENTIAL INTEGRITY CHECK
================================================================

  Total records in fact_transactions (Gold): 120,450

  [1] Orphan user_sk   :        0 rows (0.00%)  ✅ PASS
  [2] Orphan card_sk   :        0 rows (0.00%)  ✅ PASS
  [3] Orphan merch_sk  :        0 rows (0.00%)  ✅ PASS
  [4] Orphan date_sk   :        0 rows (0.00%)  ✅ PASS
  [5] NULL user_sk     :        0 rows  ✅
  [6] NULL card_sk     :        0 rows  ✅

  ─── Kết quả tổng thể: ✅ PASS — DWH nhất quán hoàn toàn ───

================================================================
  ⚡ B) QUERY PERFORMANCE BENCHMARK
================================================================

  🐢 Atomic Query (fact_transactions GROUP BY) : 4.821s
  🚀 Summary Query (agg_daily_card_fraud scan): 0.043s
  📈 Speedup Factor: 112.1x faster

  📄 Báo cáo đã được lưu tại: docs/data_quality_benchmark_report.md
```

---

## 11. Simulate SCD Type 2 (Kiểm Tra Logic Thay Đổi Dữ Liệu)

Để **demo SCD Type 2 hoạt động** với cô (không cần data thật thay đổi):

```python
# Tạo file test_scd2_demo.py
import pandas as pd

# 1. Xem trạng thái TRƯỚC khi thay đổi
df = pd.read_parquet("data/gold/dim_users/")
user_0 = df[df["user_id"] == 0][["user_id","user_sk","fico_score","is_current","effective_date","expiration_date"]]
print("TRƯỚC khi thay đổi:")
print(user_0.to_string())
# Output: 1 row, is_current=True, expiration_date=9999-12-31

# 2. Giả lập: sửa trực tiếp Silver (fico_score của user 0 thay đổi)
df_silver = pd.read_parquet("data/silver/dim_users/")
df_silver.loc[df_silver["user_id"] == 0, "fico_score"] = 800  # Giả sử tăng điểm
df_silver.to_parquet("data/silver/dim_users/part-scd2-test.parquet")

# 3. Chạy lại SCD2 script
#    spark-submit src/dwh_jobs/03_build_dim_users_scd2.py

# 4. Xem trạng thái SAU
df_after = pd.read_parquet("data/gold/dim_users/")
user_0_after = df_after[df_after["user_id"] == 0].sort_values("effective_date")
print("\nSAU khi thay đổi (SCD2 tạo 2 versions):")
print(user_0_after[["user_id","user_sk","fico_score","is_current","effective_date","expiration_date"]].to_string())
# Output: 2 rows — version cũ is_current=False, version mới is_current=True
```

---

## 12. Tắt Hệ Thống

```powershell
# Dừng nhưng giữ nguyên data:
docker compose stop

# Khởi động lại:
docker compose start

# Dừng VÀ xóa container (data Parquet vẫn còn, vì mount volume):
docker compose down

# Dừng + xóa HOÀN TOÀN kể cả volume (CẨNTHẬN: mất data Silver):
docker compose down -v
```

---

## 13. Sơ Đồ Phụ Thuộc Dữ Liệu (Dependency Graph)

```
sd254_users.csv ──► process_users.py ──► silver/dim_users/ ──► 03_scd2_users.py ──► gold/dim_users/
                                                                                            │
sd254_cards.csv ──► process_cards.py ──► silver/dim_cards/ ──► 04_scd2_cards.py ──► gold/dim_cards/
                                                                                            │
transactions.csv ──► kafka_producer ──► Kafka ──► spark_consumer ──► silver/fact/ ──► 02_merchant.py ──► gold/dim_merchant/
                                                                           │                                    │
                              (Không phụ thuộc data) ──► 01_dim_date.py ──► gold/dim_date/                     │
                                                                           │                                    │
                                                                           └──────── 05_fact_gold.py ───────────┘
                                                                                           │
                                                                                     gold/fact_transactions/
                                                                                           │
                                                                               06_summary_layers.py
                                                                                           │
                                                                     ┌─────────────────────┤
                                                                     ▼                     ▼
                                                          gold/agg_daily_...    gold/agg_monthly_...
                                                                     │
                                                          07_dq_and_benchmark.py
                                                                     │
                                                          docs/data_quality_benchmark_report.md
```

---

## 14. Debug — Lỗi Thường Gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|:---|:---|:---|
| `docker compose ps` thấy container Exited (1) | Container crash do thiếu file hoặc port bị chiếm | `docker compose logs <tên-container>` để xem lỗi cụ thể |
| `spark-batch-users` Exited (0) | **Bình thường** — job hoàn thành | Không cần làm gì |
| Dashboard 8501 trống không có data | Silver fact_transactions chưa có dữ liệu | Chờ streaming chạy ~30 giây rồi refresh |
| DWH script lỗi `Path does not exist` | Silver chưa được tạo hoặc Docker chưa chạy | Kiểm tra `data/silver/dim_users/` có tồn tại không |
| DWH script lỗi `column not found` | Tên cột Silver khác với dự kiến | Script đã tự detect tên cột — xem log dòng `Mapping cột Merchant:` |
| `07_dq_and_benchmark.py` lỗi | Gold chưa được build | Chạy `run_dwh_pipeline.ps1` trước |
| Orphan count > 0 trong DQ check | user_id trong fact không khớp dim_users | Kiểm tra Silver dim_users có đủ 2000 records không |

---

## 15. Quy Trình Vận Hành Chuẩn (End-to-End Checklist)

```
□ 1. Đặt 3 file CSV vào data/bronze/
□ 2. docker compose up -d
□ 3. Chờ 30-60 giây để tất cả service ổn định
□ 4. Mở http://localhost:8501 → thấy dashboard có dữ liệu
□ 5. Đợi streaming tích lũy đủ data (5-10 phút)
□ 6. Chạy: .\run_dwh_pipeline.ps1
□ 7. Chạy: python src/tests/07_dq_and_benchmark.py
□ 8. Xem báo cáo: docs/data_quality_benchmark_report.md
□ 9. Mở Notebook EDA: notebooks/data_analysis.ipynb
□ 10. Xong!
```
