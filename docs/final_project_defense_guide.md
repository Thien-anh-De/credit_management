# 🎓 SỔ TAY HƯỚNG DẪN BẢO VỆ ĐỒ ÁN KHO DỮ LIỆU (DWH DEFENSE GUIDE)
## Dự án: Hệ Thống Giám Sát Rủi Ro Thẻ Tín Dụng & Kho Dữ Liệu Doanh Nghiệp (Medallion Lakehouse & Kimball Star Schema)

> Tài liệu này được biên soạn dành riêng cho nhóm sinh viên trong buổi báo cáo, thuyết trình và vấn đáp trước Hội đồng Giảng viên.

---

## 📌 PHẦN 1: BỨC TRANH TOÀN CẢNH DỰ ÁN (ELEVATOR PITCH - 60 GIÂY)

Khi bước vào phòng thi, hãy mở đầu bằng lời giới thiệu ngắn gọn, tự tin và đúng trọng tâm kỹ thuật:

> *"Kính thưa Thầy/Cô, dự án của nhóm em giải quyết bài toán cốt lõi của ngành công nghệ tài chính ngân hàng: **Xây dựng một hệ thống Kỹ thuật Dữ liệu hoàn chỉnh từ đầu đến cuối (End-to-End)** kết hợp giữa **xử lý luồng thời gian thực (Streaming 24/7)** để giám sát gian lận thẻ tức thì, và **Kho Dữ liệu Doanh nghiệp (Enterprise DWH chuẩn Ralph Kimball & Bill Inmon)** phục vụ phân tích đa chiều và đối soát tài chính.*
>
> *Hệ thống được thiết kế theo kiến trúc **Medallion Lakehouse (Bronze ➔ Silver ➔ Gold)**: dữ liệu quẹt thẻ thô được trích xuất, làm sạch các định dạng lỗi (bóc ký tự $, ghép timestamp chuẩn, ép kiểu logic), sau đó nạp vào mô hình hình sao (Star Schema) với đầy đủ cơ chế lịch sử **SCD Type 1 & Type 2 (Point-in-Time Join)** và tháp tổng hợp **Inmon Roll-up (Ngày ➔ Tháng ➔ Quý)** giúp tối ưu hóa truy vấn phân tích siêu tốc."*

---

## 📋 PHẦN 2: BẢNG ĐỐI SOÁT CÁC TIÊU CHÍ ĐẦU RA CỦA ĐỒ ÁN

Khi Giảng viên hỏi: *"Đồ án của nhóm đã làm được những gì so với yêu cầu đề bài?"*, hãy mở mục này để chỉ rõ từng file minh chứng:

| STT | Đầu ra theo yêu cầu kỹ thuật | File mã nguồn / Tài liệu minh chứng | Kết quả kiểm chứng thực tế |
| :---: | :---| :---| :---|
| **1** | **Xử lý dữ liệu lớn (Big Data Lake)** | `data/bronze/credit_card_transactions-ibm_v2.csv` | Kho dữ liệu ~24 triệu giao dịch (2.35 GB thô), 2,000 khách hàng, 6,146 thẻ tín dụng |
| **2** | **Star Schema DDL & dim_date tự động** | [`docs/dwh_schema_ssms.sql`](file:///e:/credit_management/docs/dwh_schema_ssms.sql)<br>[`01_build_dim_date.py`](file:///e:/credit_management/src/dwh_jobs/01_build_dim_date.py) | DDL SQL Server với PK/FK/Index. `dim_date` sinh tự động 14,975 ngày từ 1995–2035 |
| **3** | **ETL Pipeline làm sạch dữ liệu thô** | [`src/batch_jobs/process_transactions.py`](file:///e:/credit_management/src/batch_jobs/process_transactions.py)<br>[`process_users.py`](file:///e:/credit_management/src/batch_jobs/process_users.py) | Bóc tách ký tự `$`, dấu phẩy, ghép 4 cột rời thành timestamp chuẩn, ép kiểu Parquet Snappy |
| **4** | **Streaming 24/7 phát hiện gian lận** | [`kafka_producer.py`](file:///e:/credit_management/src/streaming_jobs/kafka_producer.py)<br>[`spark_consumer.py`](file:///e:/credit_management/src/streaming_jobs/spark_consumer.py) | Máy POS phát giao dịch liên tục qua Kafka, Spark Streaming xử lý micro-batch 5s vào Silver |
| **5** | **SCD Type 1 & Type 2 (PIT Join)** | [`03_build_dim_users_scd2.py`](file:///e:/credit_management/src/dwh_jobs/03_build_dim_users_scd2.py)<br>[`04_build_dim_cards_scd2.py`](file:///e:/credit_management/src/dwh_jobs/04_build_dim_cards_scd2.py)<br>[`docs/demo_scd2.sql`](file:///e:/credit_management/docs/demo_scd2.sql) | SCD1 Merchant, SCD2 User/Card. Script demo SSMS mốc 2015-06-01 tách đúng 2 `card_sk`, 0 orphan |
| **6** | **Tầng Summary (Inmon Chapter 3)** | [`src/dwh_jobs/06_build_summary_layers.py`](file:///e:/credit_management/src/dwh_jobs/06_build_summary_layers.py) | 4 bảng: Ngày (`agg_daily_merchant_vol`) ➔ Tháng (`agg_monthly_merchant_vol`) ➔ Quý (`agg_quarterly_merchant_vol`) + Nhánh Thẻ |
| **7** | **Bộ câu truy vấn nghiệp vụ OLAP** | [`src/query_dwh.py`](file:///e:/credit_management/src/query_dwh.py)<br>Web Dashboard (Tab 3: SQL Studio) | 5 câu truy vấn mẫu ANSI SQL chạy kết quả thật trên Parquet / SQLite / Spark |
| **8** | **Kiểm thử Data Quality & Benchmark** | [`src/tests/07_dq_and_benchmark.py`](file:///e:/credit_management/src/tests/07_dq_and_benchmark.py)<br>[`docs/data_quality_benchmark_report.md`](file:///e:/credit_management/docs/data_quality_benchmark_report.md) | Đối soát Nguồn CSV vs Fact Gold = 100% Khớp (\$1,622,991.69). 0 Orphan FK. Đo speedup |
| **9** | **Từ điển dữ liệu (Data Dictionary)** | [`src/tools/generate_data_dict.py`](file:///e:/credit_management/src/tools/generate_data_dict.py)<br>[`docs/data_dictionary.md`](file:///e:/credit_management/docs/data_dictionary.md) | Quét tự động 9 bảng, mô tả đầy đủ: Kiểu dữ liệu, Nullable, Vai trò kiến trúc, Ý nghĩa |
| **10** | **Sơ đồ Mermaid (ERD, ETL, Inmon)** | [`docs/architecture_and_repo.md`](file:///e:/credit_management/docs/architecture_and_repo.md) | 3 sơ đồ Mermaid đồ họa: Star Schema `erDiagram`, Luồng ETL `flowchart TD`, Tháp Inmon |
| **11** | **Biểu đồ, Dashboard phân tích** | [`src/web_dashboard.py`](file:///e:/credit_management/src/web_dashboard.py)<br>[`notebooks/data_analysis.ipynb`](file:///e:/credit_management/notebooks/data_analysis.ipynb) | Streamlit Web Dashboard 3 tabs mượt mà + Biểu đồ Seaborn/Plotly phân tích EDA |
| **12** | **Reset demo một lệnh từ đầu** | [`reset_and_run_all.ps1`](file:///e:/credit_management/reset_and_run_all.ps1) | 1 lệnh PowerShell dựng lại sạch sẽ toàn bộ từ Bronze ➔ Silver ➔ Gold ➔ SSMS |

---

## 🎬 PHẦN 3: KỊCH BẢN THUYẾT TRÌNH TỪNG PHÚT (DEMO WALKTHROUGH 8 PHÚT)

### ⏱️ Phút 0:00 – 1:30 | Dựng lại hệ thống từ đầu (Reset Demo)
* **Thao tác:** Mở PowerShell tại thư mục gốc dự án và chạy:
  ```powershell
  .\reset_and_run_all.ps1
  ```
* **Thuyết minh:** 
  > *"Trước tiên, nhóm xin phép chạy script reset để chứng minh hệ thống đạt tính chất Tái chạy an toàn (Idempotent Pipeline). Script tự động dọn dẹp các thư mục tạm, sinh dữ liệu giả lập chèn lỗi, chạy ETL làm sạch và cách ly bản ghi lỗi, nạp vào Gold Star Schema, tính toán tháp Inmon Roll-up và cập nhật Từ điển dữ liệu."*

---

### ⏱️ Phút 1:30 – 3:00 | Sơ đồ Star Schema & Bảng Từ điển dữ liệu
* **Thao tác:** Mở tệp [`docs/architecture_and_repo.md`](file:///e:/credit_management/docs/architecture_and_repo.md) hoặc mở SSMS Database Diagrams.
* **Thuyết minh:**
  > *"Đây là mô hình Star Schema chuẩn Ralph Kimball:
  > - Bảng Fact ở mức hạt Atomic Grain: 1 dòng = 1 giao dịch quẹt thẻ tại máy POS.
  > - Kết nối với 4 bảng chiều qua các Surrogate Key tự tăng: `date_sk`, `user_sk`, `card_sk`, `merchant_sk`.
  > - Bảng `dim_date` sinh tự động 14,975 ngày từ 1995 đến 2035 bao gồm các cờ phân tích: Ngày cuối tuần, Ngày lễ Việt Nam.
  > - Toàn bộ cấu trúc cột và ý nghĩa nghiệp vụ được tự động xuất thành file Từ điển dữ liệu `docs/data_dictionary.md`."*

---

### ⏱️ Phút 3:00 – 4:30 | Khâu ETL: Xử lý dữ liệu thô, Chuẩn hóa Schema & Nén Parquet
* **Thao tác:** Mở Web Dashboard ([http://localhost:8501](http://localhost:8501)) ➔ Xem **Tab 1: Real-time Streaming** và **Tab 2: Gold OLAP Analytics**.
* **Thuyết minh:**
  > *"Dữ liệu giao dịch gốc từ IBM ở tầng Bronze vốn dĩ chưa sạch: Cột tiền dính ký tự $, dấu phẩy, thời gian bị xé lẻ thành 4 cột, nhiều giá trị địa lý bị thiếu.
  > - Pipeline ETL đã tự động bóc tách ký tự đặc biệt, ghép thành timestamp chuẩn hóa `yyyy-MM-dd HH:mm:ss`, ép kiểu Double cho số tiền và lưu trữ dưới dạng Apache Parquet nén Snappy.
  > - Định dạng cột Parquet giúp giảm 75% dung lượng đĩa và tăng tốc độ đọc truy vấn lên gấp 10 lần nhờ cơ chế Column Pruning."*

---

### ⏱️ Phút 4:30 – 6:00 | Bằng chứng thực tế SCD Type 2 & Point-in-Time Join
* **Thao tác:** Mở SSMS, mở file [`docs/demo_scd2.sql`](file:///e:/credit_management/docs/demo_scd2.sql), bôi đen và bấm `F5` thực thi từng khối:
  1. Khối 1: Xem thẻ `0_0` ở trạng thái ban đầu (chỉ 1 dòng).
  2. Khối 2: Thực hiện biến động (nâng hạn mức ngày 2015-06-01 bằng bảng tạm `#t`).
  3. Khối 3: Xem kết quả `dim_cards` sinh 2 phiên bản cũ - mới nối tiếp nhau.
  4. Khối 4: Xem giao dịch trong `fact_transactions` tự động phân tách theo đúng khoảng thời gian.
* **Thuyết minh:**
  > *"Về lịch sử biến động (SCD):
  > - Bảng `dim_merchant` áp dụng SCD Type 1 (ghi đè).
  > - Bảng `dim_cards` và `dim_users` áp dụng SCD Type 2 để theo dõi biến động hạn mức và cảnh báo rò rỉ Dark Web.
  > - Trên màn hình SSMS là bằng chứng Point-in-Time Join: Cùng một thẻ `0_0`, các giao dịch phát sinh trước mốc `2015-06-01` gắn chính xác vào `card_sk = 1` (hạn mức cũ \$24,295), các giao dịch sau mốc này gắn vào `card_sk = 6147` (hạn mức mới \$25,000). Toàn bộ giao dịch khớp đúng cửa sổ thời gian 100%, hoàn toàn không có orphan record."*

---

### ⏱️ Phút 6:00 – 7:30 | Tháp tổng hợp Inmon Roll-up & Benchmark tối ưu hóa
* **Thao tác:** Chạy bộ kiểm thử tự động:
  ```bash
  python src/tests/07_dq_and_benchmark.py
  ```
* **Thuyết minh:**
  > *"Để tối ưu hóa truy vấn phân tích, nhóm xây dựng Tháp tổng hợp chuẩn Inmon Chapter 3 cuộn từ Ngày ➔ Tháng ➔ Quý:
  > - Bảo toàn tuyệt đối các độ đo cộng dồn (Additive Measures: doanh số, số giao dịch, số vụ gian lận). Không tính trung bình của trung bình.
  > - Kết quả Benchmark cho thấy truy vấn trên bảng tổng hợp Summary nhanh hơn bảng Fact Atomic, tiết kiệm hàng triệu phép quét I/O khi mở rộng Big Data.
  > - Đối soát 4 tầng khớp 100% số lượng giao dịch (19,963) và tổng dòng tiền (\$1,622,991.69) với file nguồn Bronze."*

---

### ⏱️ Phút 7:30 – 8:00 | Web Dashboard & SQL Studio
* **Thao tác:** Trên trình duyệt ([http://localhost:8501](http://localhost:8501)), lướt qua **Tab 1 (Silver Stream)**, **Tab 2 (Gold OLAP)**, và bấm chạy 1 câu truy vấn trên **Tab 3 (SQL Studio)**.
* **Thuyết minh:**
  > *"Cuối cùng, hệ thống cung cấp Web Dashboard trực quan hóa toàn bộ chỉ số KPI, biểu đồ doanh số theo quý, rủi ro thẻ và trung tâm SQL Studio cho phép thực thi trực tiếp các câu lệnh SQL phân tích trong vài mili-giây. Nhóm xin kết thúc phần trình bày và sẵn sàng nhận câu hỏi từ Hội đồng."*

---

## 💡 PHẦN 4: BÍ KÍP VẤN ĐÁP 6 CÂU HỎI HỘI ĐỒNG (QUICK ANSWERS)

1. **Tại sao dùng Surrogate Key mà không dùng mã tự nhiên?**
   * *Đáp:* Để hỗ trợ SCD Type 2 (1 mã tự nhiên có thể có nhiều phiên bản SK), hỗ trợ tích hợp dữ liệu từ nhiều nguồn không lo trùng mã, và tăng tốc độ JOIN bằng số nguyên `BIGINT`.
2. **Tại sao phải có bảng `dim_date` riêng?**
   * *Đáp:* Để chứa sẵn các thuộc tính phân tích phong phú (Thứ, Quý, Cuối tuần, Ngày lễ VN) và tăng tốc truy vấn mà không phải chạy hàm tính toán ngày tháng (`DATEPART`) trên hàng triệu dòng Fact.
3. **Tại sao không lưu giá trị trung bình hay tỷ lệ phần trăm trong bảng Summary?**
   * *Đáp:* Vì trung bình và phần trăm là đại lượng không cộng dồn (Non-additive). Nếu cộng trung bình của các tháng lại sẽ bị sai lệch toán học nghiêm trọng. Chỉ lưu các đại lượng cộng dồn (`total_volume`, `total_txn`), giá trị trung bình được tính ở câu SELECT cuối bằng phép chia.
4. **Dữ liệu stream từ POS dừng ở Silver hay chảy vào Gold?**
   * *Đáp:* Dừng ở Silver để phục vụ giám sát tức thì 24/7 (Real-time Alerting). Định kỳ (Batch ELT), dữ liệu sạch mới được nạp vào Gold để tính toán SCD2 và cuộn tháp Inmon, tránh làm treo kho dữ liệu khi báo cáo.
5. **Làm sao chứng minh SCD Type 2 chạy thật?**
   * *Đáp:* Mở SSMS chạy file `docs/demo_scd2.sql`. Cùng một thẻ `0_0`, giao dịch được chia làm 2 giai đoạn gắn đúng 2 `card_sk` khác nhau dựa trên mốc ngày hiệu lực 2015-06-01.
6. **Làm sao kiểm soát dữ liệu không bị thất thoát khi ETL?**
   * *Đáp:* Hệ thống có bảng Audit Log ghi nhận đẳng thức bảo toàn: $\text{Số dòng đọc} = \text{Số dòng lỗi bị cách ly} + \text{Số dòng nạp sạch}$ ($2,060 = 291 + 1,769$ dòng, khớp 100%).

---

## 🛠️ PHẦN 5: CÁC LỆNH KHẨN CẤP KHI DEMO (CHEAT SHEET)

```powershell
# 1. Chạy lại toàn bộ hệ thống từ đầu:
.\reset_and_run_all.ps1

# 2. Chạy test suite kiểm thử & đối soát dòng tiền:
python src/tests/07_dq_and_benchmark.py

# 3. Demo SCD Type 2 bằng SQL thuần:
# Mở SSMS và thực thi từng khối trong docs/demo_scd2.sql

# 4. Chạy truy vấn SQL trực tiếp từ Terminal:
python src/query_dwh.py 1    # Rollup Tháng -> Quý
python src/query_dwh.py 3    # Bằng chứng SCD2 PIT Join

# 5. Mở Web Dashboard:
streamlit run src/web_dashboard.py
```

Chúc bạn và nhóm có một buổi bảo vệ thành công rực rỡ và đạt điểm số tối đa! 🌟

