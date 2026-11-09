-- ====================================================================================================
-- DDL THIẾT KẾ DATA WAREHOUSE CHUẨN KIMBALL STAR SCHEMA TRONG SQL SERVER / SSMS
-- Dự án: Credit Management & Fraud Monitoring Data Warehouse
-- Hệ thống: Star Schema + SCD Type 0/1/2 + Summary Layers (Inmon Chapter 3)
-- ====================================================================================================

-- 1. TẠO CƠ SỞ DỮ LIỆU DATA WAREHOUSE
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'CreditCard_DWH')
BEGIN
    CREATE DATABASE CreditCard_DWH;
END
GO

USE CreditCard_DWH;
GO

-- Xóa bảng cũ nếu tồn tại (theo thứ tự ràng buộc khóa ngoại)
IF OBJECT_ID('fact_transactions', 'U') IS NOT NULL DROP TABLE fact_transactions;
IF OBJECT_ID('agg_daily_merchant_vol', 'U') IS NOT NULL DROP TABLE agg_daily_merchant_vol;
IF OBJECT_ID('agg_monthly_merchant_vol', 'U') IS NOT NULL DROP TABLE agg_monthly_merchant_vol;
IF OBJECT_ID('agg_quarterly_merchant_vol', 'U') IS NOT NULL DROP TABLE agg_quarterly_merchant_vol;
IF OBJECT_ID('agg_daily_card_fraud', 'U') IS NOT NULL DROP TABLE agg_daily_card_fraud;
IF OBJECT_ID('dim_cards', 'U') IS NOT NULL DROP TABLE dim_cards;
IF OBJECT_ID('dim_users', 'U') IS NOT NULL DROP TABLE dim_users;
IF OBJECT_ID('dim_merchant', 'U') IS NOT NULL DROP TABLE dim_merchant;
IF OBJECT_ID('dim_date', 'U') IS NOT NULL DROP TABLE dim_date;
GO

-- ====================================================================================================
-- 2. CÁC BẢNG CHIỀU (DIMENSION TABLES)
-- ====================================================================================================

-- 2.1. DIM_DATE (SCD Type 0 - Bất biến)
CREATE TABLE dim_date (
    date_sk INT PRIMARY KEY,                       -- Khóa thay thế: YYYYMMDD (ví dụ 20150601)
    full_date_str VARCHAR(10) NOT NULL,            -- 'YYYY-MM-DD'
    day_of_week INT NOT NULL,                      -- 1 (CN) đến 7 (Thứ 7)
    day_name VARCHAR(15) NOT NULL,                 -- 'Sunday', 'Monday',...
    day_of_month INT NOT NULL,                     -- 1 - 31
    week_of_year INT NOT NULL,                     -- 1 - 53
    month_number INT NOT NULL,                     -- 1 - 12
    month_name VARCHAR(15) NOT NULL,               -- 'January',...
    quarter INT NOT NULL,                          -- 1, 2, 3, 4
    year INT NOT NULL,                             -- 1995 - 2035
    is_weekend BIT NOT NULL,                       -- 1: Cuối tuần, 0: Ngày thường
    is_holiday_vn BIT NOT NULL                     -- 1: Ngày lễ, 0: Bình thường
);
GO

-- 2.2. DIM_MERCHANT (SCD Type 1 - Overwrite Attributes)
CREATE TABLE dim_merchant (
    merchant_sk BIGINT PRIMARY KEY,                -- Khóa thay thế Surrogate Key
    merchant_name VARCHAR(100) NOT NULL,           -- Tên / Mã định danh thương nhân
    merchant_city VARCHAR(100) NOT NULL,           -- Thành phố
    merchant_state VARCHAR(50),                    -- Bang / Khu vực
    mcc VARCHAR(10) NOT NULL                       -- Merchant Category Code
);
GO

-- 2.3. DIM_USERS (SCD Type 2 - Add Row Versioning)
CREATE TABLE dim_users (
    user_sk BIGINT PRIMARY KEY,                    -- Khóa thay thế Surrogate Key cho từng version
    user_id BIGINT NOT NULL,                       -- Khóa tự nhiên Natural Key khách hàng
    user_name VARCHAR(100),
    gender VARCHAR(10),
    current_age INT,
    retirement_age INT,
    birth_year INT,
    birth_month INT,
    address VARCHAR(255),
    apartment VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    zipcode VARCHAR(20),
    latitude FLOAT,
    longitude FLOAT,
    per_capita_income FLOAT,
    yearly_income FLOAT,
    total_debt FLOAT,
    fico_score INT,
    num_credit_cards INT,
    row_hash VARCHAR(64),                          -- Mã băm MD5 phát hiện thay đổi thuộc tính
    effective_date DATE NOT NULL,                  -- Mốc bắt đầu hiệu lực (Sentinel: 1970-01-01)
    expiration_date DATE NOT NULL,                 -- Mốc kết thúc hiệu lực (Mở: 9999-12-31)
    is_current BIT NOT NULL                        -- 1: Đang có hiệu lực, 0: Lịch sử đã đóng
);
CREATE INDEX idx_users_pit ON dim_users(user_id, effective_date, expiration_date);
GO

-- 2.4. DIM_CARDS (SCD Type 2 - Add Row Versioning)
CREATE TABLE dim_cards (
    card_sk BIGINT PRIMARY KEY,                    -- Khóa thay thế Surrogate Key cho từng version thẻ
    card_natural_key VARCHAR(50),                  -- Khóa tự nhiên Natural Key: user_id + '_' + card_index
    user_id BIGINT NOT NULL,                       -- Tham chiếu tới khách hàng
    card_index INT NOT NULL,                       -- Thứ tự thẻ của user
    card_brand VARCHAR(50),                        -- Visa, Mastercard, Amex, Discover
    card_type VARCHAR(50),                         -- Debit, Credit, Prepaid
    card_number VARCHAR(50),
    expires VARCHAR(10),
    cvv VARCHAR(10),
    has_chip BIT,                                  -- 1: Có chip, 0: Không
    cards_issued INT,
    credit_limit FLOAT NOT NULL,                   -- Hạn mức tín dụng (SCD2 theo dõi biến động)
    acct_open_date DATE,                           -- Ngày mở thẻ thực tế
    year_pin_last_changed INT,
    card_on_dark_web BIT NOT NULL,                 -- Cảnh báo rò rỉ chợ đen (SCD2 theo dõi an ninh)
    row_hash VARCHAR(64),                          -- Mã băm MD5 phát hiện thay đổi
    effective_date DATE NOT NULL,                  -- Mốc bắt đầu hiệu lực (Phiên bản 1 dùng acct_open_date)
    expiration_date DATE NOT NULL,                 -- Mốc kết thúc hiệu lực
    is_current BIT NOT NULL                        -- 1: Bản ghi hiện tại, 0: Bản ghi lịch sử đã đóng
);
CREATE INDEX idx_cards_pit ON dim_cards(card_natural_key, effective_date, expiration_date);
GO

-- ====================================================================================================
-- 3. BẢNG SỰ KIỆN (FACT TABLE - ATOMIC GRAIN)
-- ====================================================================================================

CREATE TABLE fact_transactions (
    transaction_id VARCHAR(32) PRIMARY KEY,        -- Khóa suy biến Degenerate Key băm MD5 100% cột tự nhiên
    date_sk INT NOT NULL,                          -- Foreign Key -> dim_date
    user_sk BIGINT NOT NULL,                       -- Foreign Key -> dim_users (Point-in-Time Join)
    card_sk BIGINT NOT NULL,                       -- Foreign Key -> dim_cards (Point-in-Time Join)
    merchant_sk BIGINT NOT NULL,                   -- Foreign Key -> dim_merchant
    amount FLOAT NOT NULL,                         -- Số tiền giao dịch ($)
    is_fraud BIT NOT NULL,                         -- 1: Gian lận, 0: Hợp lệ
    use_chip VARCHAR(30),                          -- 'Swipe Transaction', 'Chip Transaction', 'Online Transaction'
    transaction_timestamp DATETIME2 NOT NULL,      -- Thời khắc phát sinh sự kiện tại máy POS

    -- Ràng buộc toàn vẹn khóa ngoại (Referential Integrity)
    CONSTRAINT fk_fact_date FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    CONSTRAINT fk_fact_merchant FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk),
    CONSTRAINT fk_fact_users FOREIGN KEY (user_sk) REFERENCES dim_users(user_sk),
    CONSTRAINT fk_fact_cards FOREIGN KEY (card_sk) REFERENCES dim_cards(card_sk)
);
CREATE INDEX idx_fact_date ON fact_transactions(date_sk);
CREATE INDEX idx_fact_card ON fact_transactions(card_sk);
CREATE INDEX idx_fact_merchant ON fact_transactions(merchant_sk);
GO

-- ====================================================================================================
-- 4. THÁP PHÂN TẦNG TỔNG HỢP SUMMARY LAYERS (INMON CHAPTER 3)
-- ====================================================================================================

-- 4.1. Tháp 1 - Tầng Ngày Merchant (Cơ sở tháp Inmon Rollup)
CREATE TABLE agg_daily_merchant_vol (
    date_sk INT NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    merchant_sk BIGINT NOT NULL,
    total_txn INT NOT NULL,
    total_volume FLOAT NOT NULL,
    fraud_count INT NOT NULL,
    avg_txn_amount FLOAT,
    fraud_rate_pct FLOAT,
    PRIMARY KEY (date_sk, merchant_sk),
    CONSTRAINT fk_agg_daily_date FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    CONSTRAINT fk_agg_daily_merch FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk)
);
GO

-- 4.2. Tháp 1 - Tầng Tháng Merchant (Cuộn từ Tầng Ngày)
CREATE TABLE agg_monthly_merchant_vol (
    year INT NOT NULL,
    quarter INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    merchant_sk BIGINT NOT NULL,
    total_txn INT NOT NULL,
    total_volume FLOAT NOT NULL,
    fraud_count INT NOT NULL,
    avg_txn_amount FLOAT,
    fraud_rate_pct FLOAT,
    PRIMARY KEY (year, month_number, merchant_sk),
    CONSTRAINT fk_agg_monthly_merch FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk)
);
GO

-- 4.3. Tháp 1 - Tầng Quý Merchant (Cuộn từ Tầng Tháng - Milestone 2.2)
CREATE TABLE agg_quarterly_merchant_vol (
    year INT NOT NULL,
    quarter INT NOT NULL,
    merchant_sk BIGINT NOT NULL,
    total_txn INT NOT NULL,
    total_volume FLOAT NOT NULL,
    fraud_count INT NOT NULL,
    avg_txn_amount FLOAT,
    fraud_rate_pct FLOAT,
    PRIMARY KEY (year, quarter, merchant_sk),
    CONSTRAINT fk_agg_quarterly_merch FOREIGN KEY (merchant_sk) REFERENCES dim_merchant(merchant_sk)
);
GO

-- 4.4. Tháp 2 - Nhánh Rủi ro Thẻ (Ngày x Thẻ)
CREATE TABLE agg_daily_card_fraud (
    date_sk INT NOT NULL,
    card_sk BIGINT NOT NULL,
    total_txn INT NOT NULL,
    total_amount FLOAT NOT NULL,
    fraud_count INT NOT NULL,
    avg_amount FLOAT,
    max_amount FLOAT NOT NULL,
    fraud_rate_pct FLOAT,
    PRIMARY KEY (date_sk, card_sk),
    CONSTRAINT fk_agg_card_date FOREIGN KEY (date_sk) REFERENCES dim_date(date_sk),
    CONSTRAINT fk_agg_card_card FOREIGN KEY (card_sk) REFERENCES dim_cards(card_sk)
);
GO

