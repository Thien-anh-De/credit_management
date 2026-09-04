# 📦 Giai đoạn 2: Xử lý Dữ liệu Lô (Batch Pipeline - Phase 2)

**Mục tiêu:** Nạp, làm sạch và chuẩn hóa các bảng dữ liệu tĩnh (Khách hàng & Thẻ tín dụng) từ tầng Bronze CSV sang tầng Silver Parquet.

---

## 🛠️ Chi tiết Công việc & Logic Code:

### 1. Xử lý Bảng Khách hàng (`src/batch_jobs/process_users.py`)
- **Đầu vào:** `data/bronze/sd254_users.csv` (2,000 dòng).
- **Các bước làm sạch:**
  1. Loại bỏ ký tự tiền tệ rác (dấu `$`) ở các cột thu nhập (`per_capita_income`, `yearly_income`, `total_debt`).
  2. Xử lý giá trị trống (Null/Missing values) bằng giá trị mặc định chuẩn.
  3. Ép kiểu dữ liệu chuẩn (`IntegerType`, `DoubleType`).
  4. Bổ sung cơ chế dọn dẹp thư mục tạm `shutil.rmtree` để tránh lỗi khóa file lock trên Windows.
- **Đầu ra:** Ghi đè file Parquet nén Snappy vào `data/silver/dim_users/`.

### 2. Xử lý Bảng Thẻ tín dụng (`src/batch_jobs/process_cards.py`)
- **Đầu vào:** `data/bronze/sd254_cards.csv` (6,146 dòng).
- **Các bước làm sạch:**
  1. Làm sạch ký tự `$` trong cột hạn mức tín dụng (`credit_limit`).
  2. Đổi giá trị `YES/NO` ở cột `card_on_dark_web` thành Boolean (`True/False`).
  3. Ép kiểu chuẩn cho các cột hạn mức, ngày mở thẻ, năm đổi PIN.
  4. Tự động làm sạch thư mục cũ trước khi ghi.
- **Đầu ra:** Ghi đè file Parquet nén Snappy vào `data/silver/dim_cards/`.

---

## 💡 Điểm cải tiến kỹ thuật quan trọng:
Để tránh tình trạng Batch Job bị nghẽn (`State: WAITING`) do luồng Streaming 24/7 chiếm dụng hết tài nguyên của Spark Master Cluster, 2 job Batch được cấu hình chạy ở chế độ **`local[*]`**. 

Nhờ đó, 2 job Batch nạp xong 2,000 người dùng và 6,146 thẻ tín dụng chỉ trong **3 giây** rồi tự động giải phóng tài nguyên.
