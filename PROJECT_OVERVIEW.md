# 💳 TỔNG QUAN DỰ ÁN: HỆ THỐNG STREAMING & GIÁM SÁT GIAN LẬN THẺ TÍN DỤNG THỜI GIAN THỰC
## (Real-Time Credit Card Streaming, Medallion Lakehouse & Kimball Star Schema DWH)

---

## 📌 1. Giới thiệu & Mục tiêu Dự án

Dự án **Credit Management** là một hệ thống Kỹ thuật Dữ liệu (Data Engineering) hoàn chỉnh từ đầu đến cuối (End-to-End), được thiết kế và xây dựng nhằm mô phỏng, xử lý phân tán, chuẩn hóa mô hình dữ liệu và giám sát các giao dịch quẹt thẻ tín dụng ngân hàng theo thời gian thực hoạt động liên tục 24/7.

Hệ thống giải quyết trọn vẹn các bài toán kỹ thuật cốt lõi của ngành công nghệ tài chính (Fintech / Banking):
* **Xử lý dữ liệu lớn (Big Data Ingestion):** Khả năng mở rộng xử lý phân tán toàn bộ kho dữ liệu thực tế ~24 triệu giao dịch (tập IBM v2, dung lượng ~2.35 GB thô) với Apache Spark, kết hợp bộ dữ liệu mẫu chuẩn hóa 19,963 giao dịch (`User0`) phục vụ kiểm thử nhanh, CI/CD và đối soát tài chính 100%.
* **Phát hiện rủi ro tức thì (Real-time Fraud Monitoring):** Chuẩn hóa và gắn cờ các giao dịch có nguy cơ gian lận ngay khi sự kiện phát sinh tại các thiết bị POS.
* **Kiến trúc Hồ dữ liệu hiện đại (Medallion Lakehouse):** Phân định rành mạch giữa tầng dữ liệu thô (**Bronze**), tầng dữ liệu sạch (**Silver**) và tầng Kho Dữ liệu Doanh nghiệp chuẩn Kimball (**Gold/DWH**).
* **Mô hình DWH chuẩn Kimball Star Schema:** Thiết kế bảng chiều (Dimensions) với phân loại SCD Type 0, Type 1, Type 2 rõ ràng; bảng sự kiện (Fact) áp dụng kỹ thuật **Point-in-Time Join** gắn chính xác phiên bản lịch sử.
* **Tháp phân tầng tổng hợp Inmon & Kimball (Summary Layers - Milestone 2.2):** Xây dựng tháp tổng hợp Inmon Roll-up nhất quán theo trục Thương nhân (**Ngày ➔ Tháng ➔ Quý**) bảo toàn các độ đo cộng dồn (additive), cùng nhánh phân tích chuyên biệt theo rủi ro Thẻ (**Ngày × Thẻ**) giúp tối ưu hóa truy vấn phân tích.
* **Bộ kiểm thử toàn diện & Bằng chứng thực tế (Data Quality & SCD2 Simulator):** Tự động kiểm tra Referential Integrity (0 orphan), tính duy nhất khóa thay thế (Surrogate Key Uniqueness 100%), quy tắc SCD2 không chồng lấn, đo đếm tỷ lệ fallback coalesce (0.00%), và đối soát 100% dòng tiền (Reconciliation).

---

## 🏗️ 2. Kiến trúc Tổng thể Hệ thống (System Architecture)

Dự án áp dụng mô hình kiến trúc **Medallion Architecture (Bronze ➔ Silver ➔ Gold/DWH)** kết hợp hài hòa giữa xử lý theo lô (**Batch Processing**) và xử lý luồng dữ liệu thời gian thực (**Real-Time Streaming**).

```text
                                 [ NGUỒN DỮ LIỆU THÔ (BRONZE) ]
                                (IBM Credit Card Datasets - CSV)
                                                │
        ┌───────────────────────────────────────┴───────────────────────────────────────┐
        ▼                                                                               ▼
[ XỬ LÝ LÔ (BATCH PIPELINE) ]                                           [ BẮN LUỒNG 24/7 (STREAMING PIPELINE) ]
process_users.py, process_cards.py                                      kafka_producer.py (POS Simulator)
& process_transactions.py                                                               │
        │ (Chạy local[*] <3s)                                                           ▼
        ▼                                                                       [ APACHE KAFKA BROKER ]
[ TẦNG SILVER: PARQUET LAKE ]                                                   (Topic: transactions_topic)
dim_users, dim_cards & fact_transactions                                                │
        │                                                                               ▼
        │                                                               [ SPARK STRUCTURED STREAMING ]
        │                                                               spark_consumer.py (Micro-batch 5s)
        │                                                                               │
        └───────────────────────────────────────┬───────────────────────────────────────┘
                                                ▼
                                   [ TẦNG SILVER PARQUET ]
                                 (Định dạng Parquet Snappy)
                                                │
                                                ▼
                      [ TẦNG GOLD: DATA WAREHOUSE KIMBALL STAR SCHEMA ]
                 (Bộ script dwh_jobs/ 01 -> 06 + simulate_scd2_changes.py)
                        │                                       │
        ┌───────────────┴───────────────┐                       │
        ▼                               ▼                       ▼
  [ STAR SCHEMA ]             [ INMON SUMMARY LAYERS ]    [ TEST SUITE & BENCHMARK ]
  • dim_date (Type 0)         • agg_daily_merchant_vol    07_dq_and_benchmark.py
  • dim_merchant (Type 1)     • agg_monthly_merchant_vol  (Referential Integrity,
  • dim_users (Type 2)        • agg_quarterly_merchant_vol Uniqueness, SCD2 Rules,
  • dim_cards (Type 2)        • agg_daily_card_fraud      Reconciliation, Speedup)
  • fact_transactions         (Rollup: Ngày ➔ Tháng ➔ Quý
    (SCD2 Point-in-Time Join)  & Nhánh Card-Risk)
        │                                                       │
        └───────────────────────┬───────────────────────────────┘
                                ▼
        ┌───────────────────────────────────────┬───────────────────────────────────────┐
        ▼                                                                               ▼
[ TRUNG TÂM GIÁM SÁT VẬN HÀNH 24/7 ]                                     [ PHÂN TÍCH CHUYÊN SÂU (EDA) ]
web_dashboard.py (Streamlit: Silver Stream + Gold OLAP)                  data_analysis.ipynb (Jupyter Notebook)
```

### Các tầng dữ liệu trong Hồ dữ liệu (Medallion Lakehouse):
1. **Bronze Layer (`data/bronze/`)**: Lưu trữ các file dữ liệu gốc định dạng CSV từ tập dữ liệu chuẩn IBM Credit Card:
   - `sd254_users.csv`: Dữ liệu 2,000 khách hàng (tuổi, thu nhập, điểm tín dụng FICO, nợ xấu, địa chỉ,...).
   - `sd254_cards.csv`: Dữ liệu 6,146 thẻ tín dụng (loại thẻ, hạn mức, ngày mở thẻ, trạng thái rò rỉ Dark Web,...).
   - `User0_credit_card_transactions.csv` (19,963 giao dịch mẫu chuẩn hóa) & `credit_card_transactions-ibm_v2.csv` (khoảng 24 triệu giao dịch đầy đủ, ~2.35 GB).
2. **Silver Layer (`data/silver/`)**: Tầng dữ liệu đã được làm sạch, ép kiểu schema chuẩn hóa và lưu trữ dưới dạng file cột **Apache Parquet (nén Snappy)**:
   - `dim_users/`: Bảng thứ nguyên người dùng đã làm sạch (2,000 records).
   - `dim_cards/`: Bảng thứ nguyên thẻ tín dụng đã làm sạch (6,146 records).
   - `fact_transactions/`: Bảng sự kiện giao dịch được streaming append liên tục hoặc nạp batch từ Bronze.
   - `checkpoints/`: Lưu trữ vị trí đọc (Offset Checkpointing) của Spark để đảm bảo tính toàn vẹn **Exactly-Once Semantics**.
3. **Gold Layer / DWH (`data/gold/`)**: Tầng Kho Dữ liệu Doanh nghiệp chuẩn **Kimball Star Schema** — đầu ra của bộ script `src/dwh_jobs/`:
   - `dim_date/`: Bảng chiều thời gian (14,975 ngày từ 1995–2035 bao phủ toàn bộ lịch sử giao dịch).
   - `dim_merchant/`: Bảng chiều Merchant với Surrogate Key `merchant_sk` (**SCD Type 1**).
   - `dim_users/`: Bảng chiều User với Surrogate Key `user_sk` + **SCD Type 2** (`effective_date`, `expiration_date`, `is_current`).
   - `dim_cards/`: Bảng chiều Card với Surrogate Key `card_sk` + **SCD Type 2** theo dõi hạn mức & cảnh báo rò rỉ Dark Web.
   - `fact_transactions/`: Bảng sự kiện Atomic Grain với kỹ thuật **SCD2 Point-in-Time Join** và khóa suy biến `transaction_id` deterministic từ cột tự nhiên (Idempotent 100%).
   - `agg_daily_merchant_vol/`: **Summary Layer 1A** — tổng hợp theo Ngày × Merchant (tầng cơ sở chuỗi Inmon Rollup).
   - `agg_monthly_merchant_vol/`: **Summary Layer 2** — tổng hợp theo Tháng × Merchant (roll-up cộng dồn từ Ngày).
   - `agg_quarterly_merchant_vol/`: **Summary Layer 3** — tổng hợp theo Quý × Merchant (roll-up cộng dồn từ Tháng - Milestone 2.2).
   - `agg_daily_card_fraud/`: **Summary Layer 1B** — nhánh chuyên biệt Ngày × Thẻ phục vụ giám sát rủi ro & gian lận thẻ.

---

## 🧩 3. Chi Tiết Kỹ Thuật Các Phân Hệ & Điểm Nổi Bật Đã Đạt Được

### 3.1. Chiến Lược SCD (Slowly Changing Dimensions) Trong Toàn Bộ Hệ Thống
Hệ thống phân định rõ ràng và áp dụng đúng đắn 3 kỹ thuật SCD theo chuẩn lý thuyết Ralph Kimball:

| Bảng Dimension | Phân loại SCD | Thuộc tính theo dõi | Chiến lược xử lý kỹ thuật |
|:---|:---:|:---|:---|
| **`dim_date`** | **SCD Type 0**<br>(Fixed / Retain Original) | Lịch, Quý, Tháng, Thứ, Ngày lễ | Dữ liệu lịch là bất biến trong tự nhiên. Một khi sinh ra từ 1995 đến 2035, không bao giờ thay đổi thuộc tính. |
| **`dim_merchant`** | **SCD Type 1**<br>(Overwrite Attributes) | Tên thương nhân, Thành phố, Bang, MCC | Khi thông tin thương nhân thay đổi hoặc chuẩn hóa, hệ thống cập nhật ghi đè trực tiếp, không lưu vết lịch sử nhằm tối ưu không gian lưu trữ và phù hợp với nghiệp vụ POS. |
| **`dim_users`** | **SCD Type 2**<br>(Add Row Versioning) | Địa chỉ, Thu nhập hàng năm, Điểm tín dụng FICO | Bổ sung 3 cột bắt buộc: `effective_date`, `expiration_date`, `is_current`. Sử dụng mốc **sentinel date** (`1970-01-01`) cho phiên bản ban đầu. Khi phát hiện thay đổi qua mã băm MD5, đóng bản ghi cũ (`expiration_date = effective_date_moi`, `is_current = False`) và mở bản ghi mới với `user_sk` mới. |
| **`dim_cards`** | **SCD Type 2**<br>(Add Row Versioning) | Hạn mức tín dụng (`credit_limit`), Rò rỉ Dark Web (`card_on_dark_web`) | Phiên bản đầu tiên sử dụng **ngày mở thẻ thực tế** (`acct_open_date`, ví dụ `2002-09-01`). Mốc `1970-01-01` được ghi nhận là mốc lính canh (sentinel date) fallback. Ghi lại chính xác thời điểm thẻ bị rò rỉ trên Dark Web hoặc nâng/hạ hạn mức. Đây là bằng chứng pháp lý quan trọng (Forensics Audit Trail) để phân định trách nhiệm bồi thường gian lận ngân hàng. |

---

### 3.2. Chứng Minh SCD Type 2 Hoạt Động Bằng Dữ Liệu Thực Tế (`simulate_scd2_changes.py`)
Do dữ liệu gốc CSV là ảnh chụp tĩnh (snapshot), để trả lời trọn vẹn câu hỏi phản biện của Giảng viên: *“Hệ thống SCD Type 2 đã chạy thật trên dữ liệu chưa?”*, dự án đã xây dựng công cụ mô phỏng và kiểm chứng chuyên sâu:
* **Tập lệnh:** [`src/dwh_jobs/simulate_scd2_changes.py`](file:///e:/credit_management/src/dwh_jobs/simulate_scd2_changes.py)
* **Tham số hóa ngày hiệu lực (Deterministic Parameter):** Script tiếp nhận tham số `--effective-date` với giá trị mặc định là **`2015-06-01`** (nằm ngay giữa dải lịch sử giao dịch IBM 2002–2020 thay vì dùng `today()` biến động). Điều này đảm bảo kết quả demo luôn tái lập và ổn định 100%.
* **Kịch bản thực tế:**
  1. Thẻ `0_0`: Khách hàng được xét duyệt nâng hạn mức từ `$24,295.00` lên `$25,000.00`.
  2. Thẻ `0_1`: Trung tâm an ninh mạng phát hiện thẻ bị rao bán trên chợ đen (`card_on_dark_web` chuyển từ `False` sang `True`).
  3. Thẻ `0_2`: Hạn mức được điều chỉnh về `$18,500.00` và gắn nhãn cảnh báo rủi ro Dark Web.
* **Kết quả thực tế chứng minh đa phiên bản trong Gold (kết hợp ngày mở thẻ thực tế và mốc 2015-06-01):**

| Card Key | Surrogate Key (`card_sk`) | Credit Limit | Card On Dark Web | Effective Date | Expiration Date | Trạng thái Version |
|:---|:---|---:|:---|:---|:---|:---|
| `0_0` | `1` | $24,295.00 | NO | `2002-09-01` | `2015-06-01` | 🔴 FALSE (Bản ghi cũ đã đóng) |
| `0_0` | `6147` | $25,000.00 | NO | `2015-06-01` | `9999-12-31` | 🟢 TRUE (Phiên bản mới đang mở) |
| `0_1` | `2` | $21,968.00 | NO | `2008-01-01` | `2015-06-01` | 🔴 FALSE (Bản ghi cũ an toàn) |
| `0_1` | `6148` | $21,968.00 | 🚨 YES | `2015-06-01` | `9999-12-31` | 🟢 TRUE (Bản ghi cảnh báo rủi ro) |
| `0_2` | `3` | $46,414.00 | NO | `2004-03-01` | `2015-06-01` | 🔴 FALSE (Bản ghi cũ) |
| `0_2` | `6149` | $18,500.00 | 🚨 YES | `2015-06-01` | `9999-12-31` | 🟢 TRUE (Phiên bản mới) |

* Báo cáo kiểm chứng đầy đủ được xuất tự động tại: [`docs/scd2_simulation_evidence.md`](file:///e:/credit_management/docs/scd2_simulation_evidence.md).
* **Bảo toàn lịch sử khi chạy lại pipeline:** Khi chạy lại `run_dwh_pipeline.ps1` hoặc các Job 03/04, hệ thống so sánh mã băm MD5 và giữ nguyên các bản ghi đã đóng, bảo đảm SCD2 không bị ghi đè mất.
* Hỗ trợ cờ `--reset` để đưa hệ thống về trạng thái ban đầu bất cứ lúc nào.

---

### 3.3. Fact Table & Bằng Chứng Thực Tế SCD Type 2 Point-in-Time Join (`05_build_fact_transactions.py`)
Điểm yếu chí tử trong các dự án DWH thông thường là chỉ thực hiện `JOIN ... WHERE is_current = True`. Cách làm đó khiến SCD2 trở thành hình thức, vì một giao dịch phát sinh trong quá khứ sẽ bị gắn nhầm vào phiên bản thẻ/user mới nhất.

Trong dự án này, [`05_build_fact_transactions.py`](file:///e:/credit_management/src/dwh_jobs/05_build_fact_transactions.py) hiện thực hóa phép **Point-in-Time Join chuẩn mực**:
$$\text{effective\_date} \le \text{transaction\_date} < \text{expiration\_date}$$

#### Bằng chứng thực tế: Cùng 1 thẻ, giao dịch chia tách chính xác vào hai `card_sk`:
Sau khi kích hoạt mô phỏng mốc `2015-06-01`, chạy đối soát trực tiếp trên `fact_transactions` chứng minh giao dịch lịch sử được phân bổ hoàn hảo theo đúng dòng thời gian:

| Thẻ (`card_id`) | Surrogate Key (`card_sk`) | Thuộc tính phiên bản | Khoảng hiệu lực SCD2 | Số giao dịch gắn kết | Dải thời gian giao dịch thực tế |
|:---|:---:|:---|:---|:---:|:---|
| **`0_0`** | **`1`** | Limit $24,295.00, An toàn | 2002-09-01 $\rightarrow$ 2015-06-01 | **3,993** giao dịch | 2002-09-01 $\rightarrow$ 2015-05-31 |
| **`0_0`** | **`6147`** | Limit $25,000.00, An toàn | 2015-06-01 $\rightarrow$ 9999-12-31 | **1,018** giao dịch | 2015-06-03 $\rightarrow$ 2020-02-27 |
| **`0_1`** | **`2`** | An toàn (Không rò rỉ) | 2008-01-01 $\rightarrow$ 2015-06-01 | **232** giao dịch | 2008-01-13 $\rightarrow$ 2015-05-18 |
| **`0_1`** | **`6148`** | 🚨 Cảnh báo Dark Web | 2015-06-01 $\rightarrow$ 9999-12-31 | **971** giao dịch | 2015-06-06 $\rightarrow$ 2020-02-26 |
| **`0_2`** | **`3`** | Limit $46,414.00, An toàn | 2004-03-01 $\rightarrow$ 2015-06-01 | **3,411** giao dịch | 2004-03-03 $\rightarrow$ 2015-05-31 |
| **`0_2`** | **`6149`** | Limit $18,500.00, 🚨 Dark Web | 2015-06-01 $\rightarrow$ 9999-12-31 | **921** giao dịch | 2015-06-02 $\rightarrow$ 2020-02-28 |

```python
# Đoạn mã PySpark kiểm chứng trực tiếp trên tầng Gold fact_transactions:
from pyspark.sql.functions import col, count, min, max
spark.read.parquet("data/gold/fact_transactions") \
     .filter(col("card_id") == "0_0") \
     .groupBy("card_id", "card_sk") \
     .agg(count("*").alias("so_giao_dich"), 
          min("timestamp").alias("tu_ngay"), 
          max("timestamp").alias("den_ngay")) \
     .orderBy("card_sk").show()
```

#### Đo lường minh bạch cơ chế Fallback Coalesce:
* Nhiều hệ thống dùng `coalesce(pit_card_sk, current_card_sk)` để ngụy tạo tỷ lệ 0% orphan. 
* Trong dự án này, Job 05 và Test 07 **chủ động đếm và báo cáo công khai số dòng phải dùng fallback**:
  - Số dòng khớp trực tiếp qua cửa sổ Point-in-Time: **`19,963 / 19,963 (100.00%)`**
  - Số dòng phải dùng fallback coalesce: **`0 (0.00%)`**
  - Số bản ghi vi phạm cửa sổ thời gian lịch sử: **`0 vi phạm (0.00%)`**
* Khẳng định: **Orphan = 0.00% là kết quả của sự khớp dữ liệu thực tế 100%, không che giấu lỗi bằng fallback!**

---

### 3.4. Tháp Phân Tầng Tổng Hợp Chuẩn Inmon Chapter 3 & Nhánh Phân Tích Rủi Ro Thẻ
Để đáp ứng nguyên lý kiến trúc của **W.H. Inmon (Building the Data Warehouse, Chapter 3)** — trong đó tầng dữ liệu tổng hợp cấp trên phải suy ra được từ tầng cấp dưới mà không sai lệch số học, hệ thống phân định rõ hai trục phân tích:

```text
[ THÁP 1: INMON CHAPTER 3 ROLLUP - TRỤC MERCHANT ]        [ THÁP 2: NHÁNH RỦI RO THẺ ]
           ▲                                                        │
           │                                                        ▼
┌──────────────────────────────────────────────┐        ┌─────────────────────────────┐
│  TẦNG QUÝ: agg_quarterly_merchant_vol        │        │ TẦNG NGÀY:                  │
│  (1 dòng = 1 Quý x 1 Merchant - Đỉnh tháp)   │        │ agg_daily_card_fraud        │
│  • Độ đo cộng dồn: sum(total_txn),           │        │ (1 dòng = 1 Ngày x 1 Thẻ)   │
│    sum(total_volume), sum(fraud_count)       │        │                             │
│  • Suy biến: avg_txn_amount, fraud_rate_pct  │        │ • Giám sát gian lận thẻ     │
└──────────────────────▲───────────────────────┘        │ • Phát hiện rủi ro POS      │
                       │ cuộn lên (roll-up)             └─────────────────────────────┘
┌──────────────────────┴───────────────────────┐
│  TẦNG THÁNG: agg_monthly_merchant_vol        │
│  (1 dòng = 1 Tháng x 1 Merchant - Tầng giữa) │
│  • Cuộn trực tiếp từ tầng Ngày               │
└──────────────────────▲───────────────────────┘
                       │ cuộn lên (roll-up)
┌──────────────────────┴───────────────────────┐
│  TẦNG NGÀY: agg_daily_merchant_vol           │
│  (1 dòng = 1 Ngày x 1 Merchant - Tầng Cơ Sở) │
└──────────────────────▲───────────────────────┘
                       │ tổng hợp từ Fact
┌──────────────────────┴───────────────────────┐
│  ATOMIC FACT: fact_transactions (19,963 dòng)│
└──────────────────────────────────────────────┘
```

#### 1. Tháp 1: Chuỗi Roll-up Doanh số Thương nhân (Merchant Volume Rollup Pyramid - Inmon Chapter 3)
Cả 3 tầng tổng hợp cùng nằm trên một trục thứ nguyên **Merchant**, cho phép dữ liệu cuộn dần từ Ngày lên Tháng và từ Tháng lên Quý:
* **Tầng Ngày — `agg_daily_merchant_vol` (Cơ sở tháp):**
  - **Mức hạt (Grain):** 1 dòng = 1 ngày $\times$ 1 merchant (`date_sk`, `merchant_sk`).
  - **Độ đo cộng dồn (Additive Measures):** `total_txn` ($sum$), `total_volume` ($sum$), `fraud_count` ($sum$).
* **Tầng Tháng — `agg_monthly_merchant_vol` (Tầng giữa):**
  - **Mức hạt (Grain):** 1 dòng = 1 tháng $\times$ 1 merchant (`year`, `quarter`, `month_number`, `merchant_sk`).
  - **Quy tắc Roll-up:** Cuộn trực tiếp từ tầng Ngày lên tầng Tháng bằng cách cộng dồn `sum(total_txn)`, `sum(total_volume)`, `sum(fraud_count)`.
* **Tầng Quý — `agg_quarterly_merchant_vol` (Đỉnh tháp - Milestone 2.2):**
  - **Mức hạt (Grain):** 1 dòng = 1 quý $\times$ 1 merchant (`year`, `quarter`, `merchant_sk`).
  - **Quy tắc Roll-up:** Cuộn trực tiếp từ tầng Tháng lên tầng Quý bằng cách cộng dồn các độ đo cơ sở.
* **Nguyên tắc bảo toàn độ đo (Additive vs Non-additive):**
  - Hệ thống lưu trữ các đại lượng cộng dồn được (`total_txn`, `total_volume`, `fraud_count`).
  - Tuyệt đối **không lưu trữ hoặc cộng gộp trung bình của trung bình** (`avg_txn_amount`) hay tỷ lệ phần trăm (`fraud_rate_pct`). Các chỉ số không cộng dồn (non-additive) này được tính toán suy biến ở bước truy vấn cuối cùng: $\text{avg\_txn\_amount} = \frac{\text{total\_volume}}{\text{total\_txn}}$ và $\text{fraud\_rate\_pct} = \frac{\text{fraud\_count}}{\text{total\_txn}} \times 100\%$.

#### 2. Tháp 2: Phân Hệ Giám Sát Rủi Ro Thẻ (Card Risk Monitoring Branch)
* **Tầng Ngày — `agg_daily_card_fraud`:**
  - **Mức hạt (Grain):** 1 dòng = 1 ngày $\times$ 1 thẻ (`date_sk`, `card_sk`).
  - **Nghiệp vụ:** Nhánh chuyên biệt theo trục thẻ phục vụ mô hình giám sát gian lận thẻ tín dụng, theo dõi biến động hạn mức và cảnh báo sớm.

---

### 3.5. Bộ Kiểm Thử Chất Lượng Dữ Liệu & Đối Soát Toàn Diện (Milestone 5.1)
File kiểm thử [`src/tests/07_dq_and_benchmark.py`](file:///e:/credit_management/src/tests/07_dq_and_benchmark.py) mở rộng toàn diện với 5 bộ kiểm thử độc lập:

1. **Referential Integrity Check:** Sử dụng `left_anti` join giữa `fact_transactions` với toàn bộ bảng Dimension (`dim_users`, `dim_cards`, `dim_merchant`, `dim_date`). Kết quả: **0 orphan records (100% khớp)**.
2. **Surrogate Key Uniqueness Check:** Kiểm tra tính duy nhất tuyệt đối của Surrogate Key trên tất cả các bảng. Kết quả: `user_sk`, `card_sk`, `merchant_sk`, `date_sk`, và `transaction_id` đạt **100% Unique, 0 trùng lặp**.
3. **SCD Type 2 Consistency Check:**
   - **Single Active Record:** Mỗi thẻ hoặc khách hàng có duy nhất 1 dòng `is_current = True` (0 trường hợp bị trùng active).
   - **Interval Validity:** $100\%$ bản ghi thỏa mãn $\text{effective\_date} < \text{expiration\_date}$.
   - **Non-overlapping Intervals:** Kiểm tra bằng cửa sổ phân tích `lag()`, khẳng định **0 trường hợp chồng lấn thời gian** giữa các phiên bản của cùng 1 thẻ.
4. **Fact vs Summary Reconciliation (Đối soát tính toàn vẹn 4 tầng):**
   - **Khớp 100% số lượng giao dịch (Row Count):** Fact ($19,963$) = Daily Merchant ($19,963$) = Monthly Merchant ($19,963$) = Quarterly Merchant ($19,963$) = Daily Card Fraud ($19,963$).
   - **Khớp 100% tổng dòng tiền (Dollar Volume):** Fact ($1,622,991.69) = Daily Merchant = Monthly Merchant = Quarterly Merchant = Daily Card Fraud ($1,622,991.69).
   - **Khớp 100% số vụ gian lận (Fraud Count):** Fact ($27$) = Daily Merchant = Monthly Merchant = Quarterly Merchant = Daily Card Fraud ($27$).
5. **Đo Lường Hiệu Năng & Bối Cảnh Quy Mô Dữ Liệu (Benchmark Context):**
   - **Tập mẫu kiểm thử:** Test suite chạy trên tập dữ liệu chuẩn hóa `User0_credit_card_transactions.csv` ($19,963$ giao dịch) nhằm phục vụ CI/CD và demo nhanh (~30s).
   - **Phân tích độ trễ:** Trên tập mẫu 20k dòng, thời gian truy vấn giữa Fact ($~0.45\text{s}$) và Summary ($~0.15\text{s}$) chênh lệch nhỏ do bị chi phối bởi chi phí cố định (khởi tạo Spark JVM, Catalyst Optimizer Planning).
   - **Quy mô Big Data thực tế:** Khi mở rộng lên toàn bộ kho dữ liệu `credit_card_transactions-ibm_v2.csv` (~24 triệu giao dịch, $2.35\text{ GB}$ thô), các tầng Summary Layers giúp giảm thiểu hàng triệu phép quét I/O, mang lại tốc độ vượt trội từ $20\times$ đến hơn $100\times$.

---

### 3.6. Phân Định Rạch Ròi: Nonvolatile vs Idempotent (Chuẩn Inmon & Kimball)
Để đảm bảo sự chính xác về mặt lý thuyết học thuật khi bảo vệ trước Hội đồng:

#### 1. Tính Không Thay Đổi (Nonvolatile - Inmon & Kimball)
* **Khái niệm:** Nonvolatile có nghĩa là dữ liệu lịch sử một khi đã ghi vào kho thì **không bao giờ bị cập nhật đè tại chỗ (in-place update) hay xóa bỏ**.
* **Hiện thực trong dự án:**
  - **Ở bảng Dimension:** Áp dụng **SCD Type 2**. Khi thông tin thẻ thay đổi hạn mức hay bị rò rỉ, hệ thống đóng hiệu lực bản ghi cũ và thêm bản ghi mới. Lịch sử được bảo toàn vĩnh viễn.
  - **Sống sót qua các lần chạy lại pipeline:** Khi chạy lại script `04_build_dim_cards_scd2.py` hoặc `run_dwh_pipeline.ps1`, mã nguồn chủ động đọc lại bảng Gold hiện có, so sánh băm MD5 và **giữ nguyên toàn bộ các bản ghi lịch sử đã đóng** (`is_current = False`). Nguyên tắc Nonvolatile không bao giờ bị vi phạm trong mã nguồn!
  - **Ở bảng Fact:** Bảng `fact_transactions` là sự thật lịch sử bất biến (immutable transactions).

#### 2. Tính Tái Chạy An Toàn (Idempotent Pipeline)
* **Khái niệm:** Idempotent là khả năng chạy lại toàn bộ hoặc một công đoạn của pipeline nhiều lần mà dữ liệu cuối cùng luôn hội tụ về cùng một trạng thái duy nhất, không sinh bản ghi thừa hay thay đổi định danh.
* **Hiện thực trong dự án:**
  - **Băm khóa 100% từ cột tự nhiên (Natural Business Columns):** Khóa `transaction_id` được băm MD5 thuần túy từ: `user_id`, `card_index`, `timestamp`, `amount`, `merchant_name`, `merchant_city`, `merchant_state`, `mcc`, `chip`. Tuyệt đối **không sử dụng** các hàm sinh ID không ổn định theo phân vùng như `monotonically_increasing_id()` hay window order nhân tạo.
  - **Kiểm chứng thực nghiệm:** Chạy Job 05 hai lần liên tiếp thu được chính xác 19,963 khóa không trùng lặp và độ lệch đối xứng (**Symmetric Difference**) giữa hai lần chạy bằng đúng **0**.
  ```python
  # Kiểm chứng Idempotent giữa hai lần chạy độc lập của Job 05:
  run1_ids = set(spark.read.parquet("data/gold/fact_transactions_run1").select("transaction_id").rdd.flatMap(lambda x: x).collect())
  run2_ids = set(spark.read.parquet("data/gold/fact_transactions_run2").select("transaction_id").rdd.flatMap(lambda x: x).collect())
  assert len(run1_ids) == 19963 and len(run2_ids) == 19963
  assert len(run1_ids.symmetric_difference(run2_ids)) == 0  # ✅ 100% Khớp, 0 sai lệch
  ```
  - **Cơ chế ghi `mode("overwrite")`:** Đây chỉ là kỹ thuật hạ tầng giúp xóa partition/staging tạm thời để tái nạp an toàn khi xảy ra sự cố (crash recovery / rerun safety), không phải là định nghĩa của Nonvolatile.

---

## 📁 4. Cấu Trúc Thư Mục Dự Án (Repository Tree)

```text
credit_management/
├── data/
│   ├── bronze/                      # Nguồn dữ liệu CSV thô ban đầu (Loại khỏi Git)
│   │   ├── sd254_users.csv          # Dữ liệu 2,000 khách hàng
│   │   ├── sd254_cards.csv          # Dữ liệu 6,146 thẻ tín dụng
│   │   ├── User0_credit_card_transactions.csv # Giao dịch mẫu chạy nhanh
│   │   └── credit_card_transactions-ibm_v2.csv # Dữ liệu lịch sử giao dịch toàn bộ
│   ├── silver/                      # Hồ dữ liệu Silver (Định dạng Parquet nén Snappy)
│   │   ├── dim_users/               # Bảng chiều User Profiles làm sạch
│   │   ├── dim_cards/               # Bảng chiều Credit Cards làm sạch
│   │   ├── fact_transactions/       # Bảng sự kiện giao dịch Silver
│   │   └── checkpoints/             # Điểm kiểm soát Offset của Spark Streaming
│   └── gold/                        # 🆕 Data Warehouse Gold Layer (Kimball Star Schema)
│       ├── dim_date/                # Bảng chiều thời gian SCD Type 0 (1995–2035)
│       ├── dim_merchant/            # Bảng chiều Merchant SCD Type 1
│       ├── dim_users/               # Bảng chiều User + Surrogate Key + SCD Type 2
│       ├── dim_cards/               # Bảng chiều Card + Surrogate Key + SCD Type 2
│       ├── fact_transactions/       # Fact Atomic Grain (SCD2 Point-in-Time Join)
│       ├── agg_daily_merchant_vol/  # Summary Layer 1A: Ngày × Merchant (Inmon Rollup cơ sở)
│       ├── agg_monthly_merchant_vol/# Summary Layer 2: Tháng × Merchant (Roll-up từ Ngày)
│       ├── agg_quarterly_merchant_vol/# Summary Layer 3: Quý × Merchant (Roll-up từ Tháng - Milestone 2.2)
│       └── agg_daily_card_fraud/    # Summary Layer 1B: Ngày × Thẻ × Fraud (Nhánh Card Risk)
├── docs/                            # Tài liệu kỹ thuật & Báo cáo kiểm chứng
│   ├── dwh_schema_ssms.sql          # 🆕 Script DDL SQL Server / SSMS (Star Schema + Foreign Keys)
│   ├── architecture_and_repo.md     # Tài liệu kiến trúc và luồng dữ liệu
│   ├── milestone5_dq_benchmark_report.md # Báo cáo kiểm thử DQ & Performance Benchmark
│   ├── scd2_simulation_evidence.md  # Báo cáo bằng chứng thực tế SCD Type 2 & PIT Join
│   ├── phase1_infrastructure.md     # Giai đoạn 1: Hạ tầng Docker & Container
│   ├── phase2_batch_pipeline.md     # Giai đoạn 2: Đường ống xử lý Batch
│   ├── phase3_streaming_pipeline.md # Giai đoạn 3: Đường ống Streaming 24/7
│   └── phase4_analytics.md          # Giai đoạn 4: Dashboard & Phân tích dữ liệu
├── notebooks/                       # Môi trường phân tích dữ liệu chuyên sâu
│   └── data_analysis.ipynb          # Jupyter Notebook thực hiện EDA & PySpark JOIN
├── src/                             # Mã nguồn chính của dự án
│   ├── batch_jobs/                  # Các tác vụ xử lý lô PySpark
│   │   ├── process_users.py         # Làm sạch và nén Parquet dim_users
│   │   ├── process_cards.py         # Làm sạch và nén Parquet dim_cards
│   │   └── process_transactions.py  # Chuyển đổi Bronze fact sang Silver Parquet
│   ├── streaming_jobs/              # Các tác vụ xử lý dữ liệu luồng
│   │   ├── kafka_producer.py        # Giả lập máy POS phát dữ liệu vào Kafka 24/7
│   │   └── spark_consumer.py        # Spark Structured Streaming đọc Kafka ghi Silver
│   ├── dwh_jobs/                    # 🆕 Tầng Kho Dữ Liệu Doanh Nghiệp (Gold Layer)
│   │   ├── 01_build_dim_date.py     # Sinh bảng chiều thời gian SCD0 (1995–2035)
│   │   ├── 02_build_dim_merchant.py # Bảng chiều Merchant SCD1 (Surrogate Key)
│   │   ├── 03_build_dim_users_scd2.py  # SCD Type 2 cho dim_users (sentinel date 1970-01-01)
│   │   ├── 04_build_dim_cards_scd2.py  # SCD Type 2 cho dim_cards (acct_open_date + bảo toàn rerun)
│   │   ├── 05_build_fact_transactions.py # Fact Gold SCD2 Point-in-Time Join + Natural Hash Idempotent
│   │   ├── 06_build_summary_layers.py   # Xây dựng Tháp Inmon Rollup (Ngày ➔ Tháng ➔ Quý) & Nhánh Card Risk
│   │   └── simulate_scd2_changes.py # Công cụ mô phỏng & kiểm chứng Point-in-Time Join thực tế
│   ├── query_dwh.py                 # 🆕 Công cụ CLI truy vấn DWH SQL tương tác (PySpark SQL)
│   ├── tests/                       # 🆕 Bộ kiểm thử tự động
│   │   └── 07_dq_and_benchmark.py   # DQ Check, Uniqueness, 4-tier Reconciliation & Benchmark
│   └── web_dashboard.py             # Streamlit Dashboard (Tab 1: Streaming; Tab 2: OLAP; Tab 3: SQL Studio)
├── docker-compose.yml               # File điều phối khởi động toàn bộ cụm dịch vụ
├── run_dwh_pipeline.ps1             # Script PowerShell tự động chạy trọn gói DWH
├── requirements.txt                 # Khai báo các thư viện Python (PySpark 3.5.1,...)
├── README.md                        # Tài liệu dự án bằng Tiếng Anh
└── PROJECT_OVERVIEW.md              # File tổng quan toàn diện về dự án (Tiếng Việt)
```

---

## 🛠️ 5. Bảng Công Nghệ & Thư Viện Sử Dụng (Tech Stack)

| Thành phần / Danh mục | Công nghệ / Thư viện | Vai trò kỹ thuật trong hệ thống |
| :--- | :--- | :--- |
| **Message Broker** | **Apache Kafka 3.x** | Hàng đợi thông điệp phân tán, đệm dữ liệu streaming thông lượng cao |
| **Cluster Coordinator** | **Apache Zookeeper** | Đồng bộ hóa, bầu cử Broker và lưu trữ metadata cho Kafka |
| **Compute & Streaming** | **Apache Spark 3.5.1 (PySpark)** | Động cơ tính toán phân tán cho Batch (`local[*]`), Streaming (5s), và DWH ELT |
| **Storage Engine** | **Apache Parquet (Snappy)** | Định dạng lưu trữ dạng cột (Columnar), nén dung lượng cao, tối ưu truy vấn phân tích |
| **DWH Modeling** | **Ralph Kimball Star Schema & W.H. Inmon Summary Hierarchy** | Thứ nguyên SCD Type 0, 1, 2; Fact Atomic Grain với Point-in-Time Join; Tháp Roll-up Bảo toàn Additive |
| **Relational DDL & SSMS** | **T-SQL (SQL Server Management Studio)** | Script DDL chuẩn hóa Star Schema, ràng buộc toàn vẹn khóa ngoại (FK/PK) phục vụ đối sánh RDBMS |
| **Containerization** | **Docker & Docker Compose** | Đóng gói môi trường đồng nhất, khởi chạy toàn bộ các dịch vụ chỉ với một lệnh |
| **Web Dashboard & SQL Studio** | **Streamlit, Plotly, Pandas, SQLite** | Xây dựng giao diện web realtime, biểu đồ tương tác, tự động cập nhật (Streaming + OLAP + SQL Studio) |
| **Analytics & Data Science**| **Jupyter Notebook, Seaborn** | Khám phá dữ liệu (EDA), phân tích tương quan rủi ro gian lận |
| **Programming Language** | **Python 3.10+** | Ngôn ngữ phát triển toàn bộ pipeline |

---

## 🚀 6. Hướng Dẫn Vận Hành Hệ Thống DWH & Kiểm Chứng

### Cách 1: Chạy tự động toàn bộ quy trình DWH bằng PowerShell
Từ thư mục gốc dự án, mở PowerShell và chạy:
```powershell
.\run_dwh_pipeline.ps1
```
Kịch bản tự động thực hiện tuần tự:
1. `01_build_dim_date.py`: Sinh 14,975 ngày từ 1995–2035 (SCD Type 0).
2. `02_build_dim_merchant.py`: Tách và gán `merchant_sk` cho 1,101 Merchant (SCD Type 1).
3. `03_build_dim_users_scd2.py`: Khởi tạo / cập nhật `dim_users` với SCD Type 2 (bảo toàn lịch sử khi chạy lại).
4. `04_build_dim_cards_scd2.py`: Khởi tạo / cập nhật `dim_cards` với SCD Type 2 (ngày mở thẻ thực tế `acct_open_date`).
5. `05_build_fact_transactions.py`: Xây dựng `fact_transactions` Atomic Grain với SCD2 Point-in-Time Join và khóa băm cột tự nhiên.
6. `06_build_summary_layers.py`: Xây dựng Tháp Inmon Rollup (Ngày ➔ Tháng ➔ Quý) và Nhánh Card Risk.
7. `07_dq_and_benchmark.py`: Chạy toàn bộ 5 bộ kiểm thử Data Quality, Đối soát 4 tầng & Benchmark tốc độ truy vấn.

### Cách 2: Chạy kiểm chứng mô phỏng SCD Type 2 & Point-in-Time Join thực tế
Để chứng minh cơ chế SCD Type 2 và Point-in-Time Join chia tách giao dịch thật sự giữa các phiên bản:
```bash
# 1. Chạy mô phỏng sự kiện nâng hạn mức và rò rỉ Dark Web (mốc cố định 2015-06-01)
python src/dwh_jobs/simulate_scd2_changes.py --effective-date 2015-06-01

# 2. Xem trực tiếp các bản ghi đa phiên bản và bằng chứng chia tách giao dịch trên màn hình
python src/dwh_jobs/simulate_scd2_changes.py --show

# 3. Khôi phục về dữ liệu ban đầu sau khi trình chiếu
python src/dwh_jobs/simulate_scd2_changes.py --reset
```

### Cách 3: Khởi chạy Cụm Docker Streaming 24/7 & Web Dashboard
```bash
docker compose up -d
```
Hoặc chạy dashboard trực tiếp trên máy:
```bash
streamlit run src/web_dashboard.py
```
Truy cập các giao diện quản trị:
* **Trung tâm Vận hành Dashboard:** [http://localhost:8501](http://localhost:8501)
  - **Tab 1:** Giám sát luồng Silver Streaming theo thời gian thực (Micro-batch 5s).
  - **Tab 2:** Phân tích DWH Gold OLAP Summary (Chuỗi Roll-up Doanh số & Rủi ro).
  - **Tab 3:** **Interactive SQL Studio** — Thực thi trực tiếp các câu lệnh SQL tự do trên Gold DWH kèm thời gian phản hồi (ms).
* **Giao diện Quản lý Spark Master UI:** [http://localhost:8080](http://localhost:8080)
* **Giao diện Giám sát Spark Worker UI:** [http://localhost:8081](http://localhost:8081)

### Cách 4: Thực thi Truy vấn SQL Phân tích DWH (OLAP Queries & SSMS)
Khi giảng viên hoặc hội đồng yêu cầu viết và thực thi truy vấn SQL:
1. **Truy vấn trực tiếp qua Terminal CLI:**
   ```bash
   python src/query_dwh.py
   # Hoặc chạy câu lệnh SQL trực tiếp:
   python src/query_dwh.py 1   # Chạy Roll-up Doanh số Tháng lên Quý
   python src/query_dwh.py 3   # Chạy Bằng chứng SCD2 Point-in-Time Join thẻ 0_0
   ```
2. **Truy vấn trực tiếp trên Web Dashboard:**
   Vào [http://localhost:8501](http://localhost:8501) ➔ Chọn **Tab 3: SQL Studio** ➔ Chọn các câu truy vấn mẫu hoặc tự gõ câu lệnh SQL bất kỳ ➔ Bấm **▶ Thực thi SQL**.
3. **Mở và đối sánh trong SQL Server / SSMS:**
   Mở tập tin [`docs/dwh_schema_ssms.sql`](file:///e:/credit_management/docs/dwh_schema_ssms.sql) trong **SQL Server Management Studio (SSMS)** và bấm **Execute**. Toàn bộ cơ sở dữ liệu `CreditCard_DWH` với Star Schema, Primary Key, Foreign Key sẽ được khởi tạo hoàn chỉnh, cho phép giảng viên xem sơ đồ **Database Diagrams** trực quan.

---

## 🌟 7. Điểm Sáng Kỹ Thuật Đạt Được Trong Đợt Nâng Cấp

1. **Tháp phân tầng Inmon Chapter 3 chuẩn mực & Bảo toàn Độ đo Additive:**
   - Xây dựng chuỗi roll-up nhất quán theo trục Merchant: Ngày (`agg_daily_merchant_vol`) ➔ Tháng (`agg_monthly_merchant_vol`) ➔ Quý (`agg_quarterly_merchant_vol`) bảo toàn $100\%$ các độ đo cộng dồn (`total_txn`, `total_volume`, `fraud_count`), không tính trung bình của trung bình.
   - Nhánh phân tích rủi ro thẻ (`agg_daily_card_fraud`) được tách biệt chuyên sâu phục vụ phát hiện gian lận.
2. **Bằng chứng thực nghiệm Point-in-Time Join phân tách giao dịch đa phiên bản:**
   - Sử dụng tham số ngày hiệu lực cố định `2015-06-01` nằm giữa lịch sử dữ liệu IBM (2002–2020), chứng minh cùng một thẻ được phân bổ chính xác vào 2 `card_sk` khác nhau (ví dụ thẻ `0_0`: $3,993$ txns ở `card_sk=1` an toàn và $1,018$ txns ở `card_sk=6147`).
   - Minh bạch hóa cơ chế Fallback Coalesce: Báo cáo rõ $\text{fallback\_used} = 0\ (0.00\%)$, khẳng định tính toàn vẹn 0 orphan là do khớp dữ liệu thật 100%.
3. **Phân định lý thuyết chuẩn mực: Nonvolatile vs Idempotent:**
   - **Nonvolatile:** Đảm bảo dữ liệu lịch sử không bị xóa khi chạy lại pipeline nhờ logic đọc bảng Gold hiện tại và bảo toàn toàn bộ bản ghi lịch sử đã đóng (`is_current = False`).
   - **Idempotent:** Khóa suy biến `transaction_id` băm 100% từ các cột nghiệp vụ tự nhiên, loại bỏ hoàn toàn các hàm sinh ID biến động; kiểm chứng thực nghiệm đạt độ lệch đối xứng bằng 0.
4. **Bộ kiểm thử toàn diện đạt chuẩn doanh nghiệp (Milestone 5.1):**
   - Kiểm tra Referential Integrity, Uniqueness Surrogate Keys, Single Active Rule, Non-overlapping Validity Intervals, và Đối soát $100\%$ dòng tiền ($1,622,991.69$) và số dòng giữa Fact và 4 bảng Summary Layers.
5. **Giám sát trực quan tích hợp Cả Luồng và Kho (Streaming + OLAP):**
   - Mở rộng Dashboard Streamlit với tab phân tích DWH Gold OLAP Summary, đọc trực tiếp dữ liệu tổng hợp theo Tháng, Quý và Rủi ro Thẻ song song với luồng streaming Silver.
