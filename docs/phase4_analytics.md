# 📊 Giai đoạn 4: Trực quan hóa & Giám sát Thời Gian Thực (Phase 4)

**Mục tiêu:** Xây dựng Trung tâm Giám sát Web Operations Center hiển thị KPI, tốc độ nạp dữ liệu và phát hiện giao dịch gian lận thời gian thực.

---

## 🛠️ Chi tiết Công việc & Công cụ:

### 1. Trung tâm Giám sát Web Dashboard (`src/web_dashboard.py`)
- **Công cụ:** Streamlit & Plotly (Chạy tại cổng `http://localhost:8501`).
- **Giao diện Modern Dark Mode (Glassmorphism):**
  - **Banner Trạng thái Luồng:** Hiển thị trạng thái Spark Engine (`🟢 ACTIVE`) và thời gian của giao dịch mới nhất nạp vào hệ thống.
  - **5 Thẻ KPI Thống kê:**
    1. 👥 **Khách hàng (`dim_users`):** 2,000
    2. 💳 **Thẻ tín dụng (`dim_cards`):** 6,146
    3. ⚡ **Tổng giao dịch (`fact_transactions`):** 60,000+ (Tự động tăng)
    4. 💵 **Tổng dòng tiền ($):** Tổng doanh số quẹt thẻ
    5. 🚨 **Cảnh báo Gian lận:** Số lượng & Tỷ lệ % giao dịch gian lận
  - **Biểu đồ Đường (Tốc độ nạp Real-Time):** Theo dõi lưu lượng giao dịch nạp vào hệ thống theo từng phút/giây.
  - **Biểu đồ Tròn (Cơ cấu hình thức quẹt thẻ):** Tỷ lệ Chip vs Swipe vs Online Transaction.
  - **Bảng Live Feed Cảnh Báo:** Tự động **tô đỏ các dòng giao dịch nghi vấn gian lận (`is_fraud == True`)** giúp nhân viên vận hành xử lý ngay lập tức.
  - **Tự động làm mới (Auto-Refresh):** Cập nhật dữ liệu đĩa Parquet mỗi 3 giây.

### 2. Phân tích Chuyên sâu với Jupyter Notebook (`notebooks/data_analysis.ipynb`)
- Đọc trực tiếp 3 bảng Parquet từ `data/silver/`.
- Thực hiện các truy vấn SQL phân tích chuyên sâu (ví dụ: nhóm tuổi khách hàng có tỷ lệ gian lận cao nhất, phân tích hạn mức thẻ theo thu nhập).

---

## 💡 Kết quả đạt được:
Hệ thống cung cấp trải nghiệm trực quan hóa ấn tượng, cho phép giám sát toàn bộ luồng giao dịch ngân hàng theo thời gian thực 24/7.
