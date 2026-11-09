# 📖 TỪ ĐIỂN DỮ LIỆU TOÀN DIỆN (ENTERPRISE DATA DICTIONARY)
## Dự án: Credit Management & Fraud Monitoring Data Warehouse
> Tự động sinh từ cấu trúc Parquet thực tế ngày: `2026-11-09 20:47:56`

---

## 📌 1. Bảng Tổng Hợp Danh Mục Các Bảng (Entity Catalog)

| STT | Tên Bảng | Phân Tầng (Layer) | Phân Loại Thực Thể | Chiến Lược SCD / Vai Trò | Trạng Thái Dữ Liệu |
|:---:|:---|:---|:---|:---|:---:|
| 1 | [`fact_transactions`](#fact_transactions) | Gold (DWH) | Fact Table (Atomic Grain) | Immutable Events (Nonvolatile) | ✅ Sẵn sàng (141,704 dòng) |
| 2 | [`dim_date`](#dim_date) | Gold (DWH) | Dimension Table | SCD Type 0 (Fixed / Retain Original) | ✅ Sẵn sàng (14,975 dòng) |
| 3 | [`dim_merchant`](#dim_merchant) | Gold (DWH) | Dimension Table | SCD Type 1 (Overwrite Attributes) | ✅ Sẵn sàng (5,326 dòng) |
| 4 | [`dim_users`](#dim_users) | Gold (DWH) | Dimension Table | SCD Type 2 (Add Row Versioning) | ✅ Sẵn sàng (2,000 dòng) |
| 5 | [`dim_cards`](#dim_cards) | Gold (DWH) | Dimension Table | SCD Type 2 (Add Row Versioning) | ✅ Sẵn sàng (6,149 dòng) |
| 6 | [`agg_daily_merchant_vol`](#agg_daily_merchant_vol) | Gold (Summary Layer 1A) | Inmon Summary (Base Layer) | Pre-aggregated Rollup | ✅ Sẵn sàng (5,326 dòng) |
| 7 | [`agg_monthly_merchant_vol`](#agg_monthly_merchant_vol) | Gold (Summary Layer 2) | Inmon Summary (Middle Layer) | Pre-aggregated Rollup | ✅ Sẵn sàng (5,326 dòng) |
| 8 | [`agg_quarterly_merchant_vol`](#agg_quarterly_merchant_vol) | Gold (Summary Layer 3) | Inmon Summary (Top Layer) | Pre-aggregated Rollup | ✅ Sẵn sàng (5,326 dòng) |
| 9 | [`agg_daily_card_fraud`](#agg_daily_card_fraud) | Gold (Summary Layer 1B) | Risk Analytics Summary | Pre-aggregated Analytics | ✅ Sẵn sàng (21 dòng) |
| 10 | [`dim_users (Silver)`](#dim_users-silver) | Silver (Clean Lake) | Standardized Staging | Snapshot Staging | ✅ Sẵn sàng (2,000 dòng) |
| 11 | [`dim_cards (Silver)`](#dim_cards-silver) | Silver (Clean Lake) | Standardized Staging | Snapshot Staging | ✅ Sẵn sàng (6,146 dòng) |

---

## 📋 2. Chi Tiết Cấu Trúc Schema & Từ Điển Cột Từng Bảng

### <a id="fact_transactions"></a>📊 Bảng: `fact_transactions`
- **Phân tầng:** Gold (DWH)
- **Phân loại:** Fact Table (Atomic Grain)
- **Chiến lược SCD / Mức hạt:** Immutable Events (Nonvolatile)
- **Mô tả nghiệp vụ:** Bảng sự kiện giao dịch quẹt thẻ ở mức hạt chi tiết nhất (1 dòng = 1 lần quẹt tại POS). Gắn Surrogate Keys thông qua SCD Type 2 Point-in-Time Join.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `transaction_id` | `VARCHAR(32)` | PK / Degenerate Key | **Không (NOT NULL)** | Mã băm MD5 tất cả trường tự nhiên, bảo đảm Idempotent 100% |
| `date_sk` | `INT` | FK -> dim_date.date_sk | **Không (NOT NULL)** | Khóa thay thế ngày giao dịch YYYYMMDD |
| `user_sk` | `BIGINT` | FK -> dim_users.user_sk (PIT) | **Không (NOT NULL)** | Khóa thay thế phiên bản khách hàng tại thời điểm quẹt |
| `card_sk` | `BIGINT` | FK -> dim_cards.card_sk (PIT) | **Không (NOT NULL)** | Khóa thay thế phiên bản thẻ tại thời điểm quẹt |
| `merchant_sk` | `BIGINT` | FK -> dim_merchant.merchant_sk | **Không (NOT NULL)** | Khóa thay thế điểm bán / thương nhân POS |
| `amount` | `DOUBLE` | Fact Measure (Additive) | **Không (NOT NULL)** | Số tiền quẹt thẻ ($) |
| `is_fraud` | `BOOLEAN` | Fact Measure (Additive / Flag) | **Không (NOT NULL)** | Cờ cảnh báo gian lận (True = 1, False = 0) |
| `use_chip` | `VARCHAR(30)` | Degenerate Dimension | Có (Nullable) | Hình thức giao dịch: Swipe Transaction / Chip Transaction / Online |
| `transaction_timestamp` | `TIMESTAMP` | Event Timestamp | **Không (NOT NULL)** | Thời điểm giao dịch phát sinh chính xác tại POS |

---

### <a id="dim_date"></a>📊 Bảng: `dim_date`
- **Phân tầng:** Gold (DWH)
- **Phân loại:** Dimension Table
- **Chiến lược SCD / Mức hạt:** SCD Type 0 (Fixed / Retain Original)
- **Mô tả nghiệp vụ:** Bảng chiều thời gian lịch chuẩn từ 1995 đến 2035 (14,975 ngày), bao phủ toàn bộ lịch sử ngân hàng.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `date_sk` | `INT` | Primary Key (Surrogate) | **Không (NOT NULL)** | Khóa ngày dạng số nguyên YYYYMMDD (ví dụ 20261109) |
| `full_date_str` | `VARCHAR(10)` | Attribute | **Không (NOT NULL)** | Ngày dạng chuỗi chuẩn YYYY-MM-DD |
| `day_of_week` | `INT` | Attribute | **Không (NOT NULL)** | Thứ trong tuần: 1 (Chủ Nhật) đến 7 (Thứ Bảy) |
| `day_name` | `VARCHAR(15)` | Attribute | **Không (NOT NULL)** | Tên thứ tiếng Anh (Monday, Tuesday...) |
| `day_of_month` | `INT` | Attribute | **Không (NOT NULL)** | Ngày trong tháng (1 - 31) |
| `week_of_year` | `INT` | Attribute | **Không (NOT NULL)** | Tuần trong năm (1 - 53) |
| `month_number` | `INT` | Attribute | **Không (NOT NULL)** | Tháng số (1 - 12) |
| `month_name` | `VARCHAR(15)` | Attribute | **Không (NOT NULL)** | Tên tháng tiếng Anh (January, February...) |
| `quarter` | `INT` | Attribute | **Không (NOT NULL)** | Quý trong năm (1, 2, 3, 4) |
| `year` | `INT` | Attribute | **Không (NOT NULL)** | Năm theo lịch (1995 - 2035) |
| `is_weekend` | `BOOLEAN` | Analytical Flag | **Không (NOT NULL)** | Cờ đánh dấu ngày cuối tuần (Thứ 7 hoặc Chủ Nhật) |
| `is_holiday_vn` | `BOOLEAN` | Analytical Flag | **Không (NOT NULL)** | Cờ đánh dấu các ngày lễ lớn tại Việt Nam |

---

### <a id="dim_merchant"></a>📊 Bảng: `dim_merchant`
- **Phân tầng:** Gold (DWH)
- **Phân loại:** Dimension Table
- **Chiến lược SCD / Mức hạt:** SCD Type 1 (Overwrite Attributes)
- **Mô tả nghiệp vụ:** Bảng chiều thông tin đối tác chấp nhận thẻ POS / Thương nhân. Khi cập nhật thông tin thương nhân sẽ ghi đè trực tiếp.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `merchant_sk` | `BIGINT` | Primary Key (Surrogate) | **Không (NOT NULL)** | Khóa thay thế duy nhất cho từng điểm bán |
| `merchant_name` | `VARCHAR(100)` | Natural Key / Name | **Không (NOT NULL)** | Tên hoặc mã định danh thương nhân POS |
| `merchant_city` | `VARCHAR(100)` | Attribute | **Không (NOT NULL)** | Thành phố đặt máy POS (mặc định 'Unknown') |
| `merchant_state` | `VARCHAR(50)` | Attribute | **Không (NOT NULL)** | Bang hoặc khu vực địa lý |
| `mcc` | `VARCHAR(10)` | Attribute | **Không (NOT NULL)** | Mã phân loại ngành hàng Merchant Category Code |

---

### <a id="dim_users"></a>📊 Bảng: `dim_users`
- **Phân tầng:** Gold (DWH)
- **Phân loại:** Dimension Table
- **Chiến lược SCD / Mức hạt:** SCD Type 2 (Add Row Versioning)
- **Mô tả nghiệp vụ:** Bảng chiều khách hàng lưu vết lịch sử biến động địa chỉ, thu nhập năm và điểm tín dụng FICO.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `user_sk` | `BIGINT` | Primary Key (Surrogate) | **Không (NOT NULL)** | Khóa thay thế định danh duy nhất cho từng phiên bản khách hàng |
| `user_id` | `BIGINT` | Natural Key | **Không (NOT NULL)** | Mã khách hàng tự nhiên từ hệ thống Core Banking |
| `user_name` | `VARCHAR(100)` | Attribute | Có (Nullable) | Họ tên đầy đủ của khách hàng |
| `gender` | `VARCHAR(10)` | Attribute | Có (Nullable) | Giới tính (Female / Male) |
| `current_age` | `INT` | Attribute | Có (Nullable) | Tuổi hiện tại của khách hàng |
| `retirement_age` | `INT` | Attribute | Có (Nullable) | Tuổi nghỉ hưu dự kiến |
| `birth_year` | `INT` | Attribute | Có (Nullable) | Năm sinh |
| `birth_month` | `INT` | Attribute | Có (Nullable) | Tháng sinh |
| `address` | `VARCHAR(255)` | SCD2 Tracked Attribute | Có (Nullable) | Địa chỉ cư trú (theo dõi biến động SCD2) |
| `apartment` | `VARCHAR(50)` | Attribute | Có (Nullable) | Số phòng / Căn hộ |
| `city` | `VARCHAR(100)` | Attribute | Có (Nullable) | Thành phố cư trú |
| `state` | `VARCHAR(50)` | Attribute | Có (Nullable) | Bang cư trú |
| `zipcode` | `VARCHAR(20)` | Attribute | Có (Nullable) | Mã bưu chính Zipcode |
| `latitude` | `DOUBLE` | Geo Attribute | Có (Nullable) | Vĩ độ địa lý |
| `longitude` | `DOUBLE` | Geo Attribute | Có (Nullable) | Kinh độ địa lý |
| `per_capita_income` | `DOUBLE` | Attribute | Có (Nullable) | Thu nhập bình quân đầu người tại khu vực zipcode ($) |
| `yearly_income` | `DOUBLE` | SCD2 Tracked Attribute | Có (Nullable) | Thu nhập hàng năm của cá nhân (theo dõi biến động SCD2) ($) |
| `total_debt` | `DOUBLE` | Attribute | Có (Nullable) | Tổng dư nợ hiện tại của khách hàng ($) |
| `fico_score` | `INT` | SCD2 Tracked Attribute | Có (Nullable) | Điểm tín dụng FICO Score (theo dõi biến động SCD2) |
| `num_credit_cards` | `INT` | Attribute | Có (Nullable) | Tổng số lượng thẻ tín dụng đang sở hữu |
| `effective_date` | `DATE` | SCD2 PIT Control | **Không (NOT NULL)** | Ngày bắt đầu có hiệu lực của phiên bản (Sentinel: 1970-01-01) |
| `expiration_date` | `DATE` | SCD2 PIT Control | **Không (NOT NULL)** | Ngày hết hạn hiệu lực của phiên bản (Mặc định: 9999-12-31) |
| `is_current` | `BOOLEAN` | SCD2 Current Flag | **Không (NOT NULL)** | Cờ nhận diện bản ghi hiện hành (True = Đang dùng) |
| `row_hash` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---

### <a id="dim_cards"></a>📊 Bảng: `dim_cards`
- **Phân tầng:** Gold (DWH)
- **Phân loại:** Dimension Table
- **Chiến lược SCD / Mức hạt:** SCD Type 2 (Add Row Versioning)
- **Mô tả nghiệp vụ:** Bảng chiều thẻ tín dụng lưu vết lịch sử biến động hạn mức (credit_limit) và cảnh báo rò rỉ Dark Web (card_on_dark_web).

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `card_sk` | `BIGINT` | Primary Key (Surrogate) | **Không (NOT NULL)** | Khóa thay thế định danh duy nhất cho từng phiên bản thẻ |
| `card_natural_key` | `VARCHAR(50)` | Natural Key | **Không (NOT NULL)** | Khóa tự nhiên kết hợp: user_id + '_' + card_index |
| `user_id` | `BIGINT` | Foreign Key / Natural | **Không (NOT NULL)** | Mã khách hàng sở hữu thẻ |
| `card_index` | `INT` | Natural Key Part | **Không (NOT NULL)** | Thứ tự số thẻ của khách hàng (0, 1, 2...) |
| `card_brand` | `VARCHAR(50)` | Attribute | Có (Nullable) | Thương hiệu thẻ: Visa, Mastercard, Amex, Discover |
| `card_type` | `VARCHAR(50)` | Attribute | Có (Nullable) | Loại thẻ: Credit, Debit, Prepaid |
| `card_number` | `VARCHAR(30)` | Attribute | Có (Nullable) | Số thẻ tín dụng |
| `expires` | `VARCHAR(10)` | Attribute | Có (Nullable) | Ngày hết hạn in trên thẻ (MM/YYYY) |
| `cvv` | `VARCHAR(10)` | Attribute | Có (Nullable) | Mã bảo mật thẻ CVV |
| `has_chip` | `BOOLEAN` | Attribute | **Không (NOT NULL)** | Thẻ có tích hợp chip EMV hay không |
| `cards_issued` | `INT` | Attribute | Có (Nullable) | Số lượng phôi thẻ vật lý đã phát hành |
| `credit_limit` | `DOUBLE` | SCD2 Tracked Attribute | **Không (NOT NULL)** | Hạn mức tín dụng được cấp (theo dõi nâng/hạ hạn mức) ($) |
| `acct_open_date` | `VARCHAR(10)` | Attribute | Có (Nullable) | Ngày mở tài khoản thẻ |
| `year_pin_last_changed` | `INT` | Attribute | Có (Nullable) | Năm gần nhất khách hàng đổi mã PIN |
| `card_on_dark_web` | `BOOLEAN` | SCD2 Tracked Attribute | **Không (NOT NULL)** | Cảnh báo thẻ bị lộ lọt trên Dark Web (Forensics Audit) |
| `effective_date` | `DATE` | SCD2 PIT Control | **Không (NOT NULL)** | Ngày bắt đầu có hiệu lực của phiên bản thẻ |
| `expiration_date` | `DATE` | SCD2 PIT Control | **Không (NOT NULL)** | Ngày hết hạn hiệu lực của phiên bản thẻ |
| `is_current` | `BOOLEAN` | SCD2 Current Flag | **Không (NOT NULL)** | Cờ nhận diện bản ghi thẻ hiện hành (True = Đang hiệu lực) |
| `row_hash` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---

### <a id="agg_daily_merchant_vol"></a>📊 Bảng: `agg_daily_merchant_vol`
- **Phân tầng:** Gold (Summary Layer 1A)
- **Phân loại:** Inmon Summary (Base Layer)
- **Chiến lược SCD / Mức hạt:** Pre-aggregated Rollup
- **Mô tả nghiệp vụ:** Tổng hợp doanh số và rủi ro theo Ngày x Merchant từ bảng Fact Atomic. Tầng cơ sở của Tháp Rollup Inmon.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `date_sk` | `INT` | Compound PK / FK -> dim_date | **Không (NOT NULL)** | Khóa ngày YYYYMMDD |
| `merchant_sk` | `BIGINT` | Compound PK / FK -> dim_merchant | **Không (NOT NULL)** | Khóa thương nhân POS |
| `total_txn` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng số lượt quẹt thẻ trong ngày |
| `total_volume` | `DOUBLE` | Additive Measure | **Không (NOT NULL)** | Tổng doanh số quẹt thẻ trong ngày ($) |
| `fraud_count` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng số giao dịch bị gắn cờ gian lận |
| `avg_txn_amount` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Giá trị trung bình một giao dịch ($) |
| `fraud_rate_pct` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Tỷ lệ gian lận theo phần trăm (%) |
| `year` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `quarter` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `month_number` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `month_name` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---

### <a id="agg_monthly_merchant_vol"></a>📊 Bảng: `agg_monthly_merchant_vol`
- **Phân tầng:** Gold (Summary Layer 2)
- **Phân loại:** Inmon Summary (Middle Layer)
- **Chiến lược SCD / Mức hạt:** Pre-aggregated Rollup
- **Mô tả nghiệp vụ:** Tổng hợp doanh số và rủi ro theo Tháng x Merchant. Roll-up trực tiếp từ Tầng Ngày (Inmon Chapter 3).

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `year` | `INT` | Compound PK / Partition | **Không (NOT NULL)** | Năm tổng hợp |
| `month_number` | `INT` | Compound PK | **Không (NOT NULL)** | Tháng trong năm (1 - 12) |
| `quarter` | `INT` | Attribute | **Không (NOT NULL)** | Quý trong năm (1, 2, 3, 4) |
| `merchant_sk` | `BIGINT` | Compound PK / FK -> dim_merchant | **Không (NOT NULL)** | Khóa thương nhân POS |
| `total_txn` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng lượt quẹt thẻ trong tháng |
| `total_volume` | `DOUBLE` | Additive Measure | **Không (NOT NULL)** | Tổng doanh số quẹt thẻ trong tháng ($) |
| `fraud_count` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng số giao dịch gian lận trong tháng |
| `avg_txn_amount` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Giá trị trung bình một giao dịch trong tháng ($) |
| `fraud_rate_pct` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Tỷ lệ gian lận trong tháng (%) |
| `month_name` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---

### <a id="agg_quarterly_merchant_vol"></a>📊 Bảng: `agg_quarterly_merchant_vol`
- **Phân tầng:** Gold (Summary Layer 3)
- **Phân loại:** Inmon Summary (Top Layer)
- **Chiến lược SCD / Mức hạt:** Pre-aggregated Rollup
- **Mô tả nghiệp vụ:** Tổng hợp doanh số và rủi ro theo Quý x Merchant (Đỉnh Tháp Rollup Inmon). Cuộn trực tiếp từ Tầng Tháng.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `year` | `INT` | Compound PK / Partition | **Không (NOT NULL)** | Năm tổng hợp |
| `quarter` | `INT` | Compound PK | **Không (NOT NULL)** | Quý trong năm (1, 2, 3, 4) |
| `merchant_sk` | `BIGINT` | Compound PK / FK -> dim_merchant | **Không (NOT NULL)** | Khóa thương nhân POS |
| `total_txn` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng lượt quẹt thẻ trong quý |
| `total_volume` | `DOUBLE` | Additive Measure | **Không (NOT NULL)** | Tổng doanh số trong quý ($) |
| `fraud_count` | `INT` | Additive Measure | **Không (NOT NULL)** | Tổng số giao dịch gian lận trong quý |
| `avg_txn_amount` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Giá trị trung bình một giao dịch trong quý ($) |
| `fraud_rate_pct` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Tỷ lệ gian lận trong quý (%) |

---

### <a id="agg_daily_card_fraud"></a>📊 Bảng: `agg_daily_card_fraud`
- **Phân tầng:** Gold (Summary Layer 1B)
- **Phân loại:** Risk Analytics Summary
- **Chiến lược SCD / Mức hạt:** Pre-aggregated Analytics
- **Mô tả nghiệp vụ:** Tổng hợp chỉ số rủi ro Ngày x Thẻ phục vụ bộ phận Fraud Investigation giám sát thẻ bị tấn công dồn dập.

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `date_sk` | `INT` | Compound PK / FK -> dim_date | **Không (NOT NULL)** | Khóa ngày YYYYMMDD |
| `card_sk` | `BIGINT` | Compound PK / FK -> dim_cards | **Không (NOT NULL)** | Khóa thay thế phiên bản thẻ tín dụng |
| `total_txn` | `INT` | Additive Measure | **Không (NOT NULL)** | Số lượt quẹt của thẻ trong ngày |
| `total_amount` | `DOUBLE` | Additive Measure | **Không (NOT NULL)** | Tổng số tiền quẹt trong ngày ($) |
| `fraud_count` | `INT` | Additive Measure | **Không (NOT NULL)** | Số giao dịch bị phát hiện gian lận |
| `avg_amount` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Số tiền quẹt trung bình ($) |
| `max_amount` | `DOUBLE` | Max Measure | **Không (NOT NULL)** | Giao dịch lớn nhất trong ngày ($) |
| `fraud_rate_pct` | `DOUBLE` | Derived Measure | **Không (NOT NULL)** | Tỷ lệ gian lận của thẻ (%) |

---

### <a id="dim_users-silver"></a>📊 Bảng: `dim_users (Silver)`
- **Phân tầng:** Silver (Clean Lake)
- **Phân loại:** Standardized Staging
- **Chiến lược SCD / Mức hạt:** Snapshot Staging
- **Mô tả nghiệp vụ:** Bảng hồ sơ khách hàng đã bóc tách ký tự $, chuẩn hóa snake_case và nén Parquet Snappy (2,000 khách hàng).

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `user_id` | `BIGINT` | Natural Key | **Không (NOT NULL)** | Mã khách hàng tự nhiên |
| `user_name` | `VARCHAR(100)` | Clean String | Có (Nullable) | Họ và tên |
| `per_capita_income` | `DOUBLE` | Clean Numeric | Có (Nullable) | Thu nhập bình quân zipcode |
| `yearly_income` | `DOUBLE` | Clean Numeric | Có (Nullable) | Thu nhập năm cá nhân |
| `total_debt` | `DOUBLE` | Clean Numeric | Có (Nullable) | Tổng dư nợ |
| `fico_score` | `INT` | Clean Numeric | Có (Nullable) | Điểm tín dụng FICO |
| `gender` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `current_age` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `retirement_age` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `birth_year` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `birth_month` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `address` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `apartment` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `city` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `state` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `zipcode` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `latitude` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `longitude` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `num_credit_cards` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---

### <a id="dim_cards-silver"></a>📊 Bảng: `dim_cards (Silver)`
- **Phân tầng:** Silver (Clean Lake)
- **Phân loại:** Standardized Staging
- **Chiến lược SCD / Mức hạt:** Snapshot Staging
- **Mô tả nghiệp vụ:** Bảng thẻ tín dụng đã làm sạch, ép kiểu Boolean cho Has Chip/Dark Web và nén Parquet Snappy (6,146 thẻ).

| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |
|:---|:---|:---|:---:|:---|
| `user_id` | `BIGINT` | Natural FK | **Không (NOT NULL)** | Mã khách hàng sở hữu thẻ |
| `card_index` | `INT` | Natural Key Part | **Không (NOT NULL)** | Thứ tự thẻ |
| `credit_limit` | `DOUBLE` | Clean Numeric | **Không (NOT NULL)** | Hạn mức tín dụng |
| `card_on_dark_web` | `BOOLEAN` | Clean Boolean | **Không (NOT NULL)** | Cảnh báo rò rỉ Dark Web |
| `card_brand` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `card_type` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `card_number` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `expires` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `cvv` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `has_chip` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `cards_issued` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `acct_open_date` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |
| `year_pin_last_changed` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |

---
