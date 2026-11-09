-- ====================================================================================================
-- CÁC CÂU TRUY VẤN MẪU ĐIỂN HÌNH (T-SQL TRÊN MICROSOFT SQL SERVER / SSMS)
-- Dự án: Credit Management & Fraud Monitoring Data Warehouse
-- Chạy trên cơ sở dữ liệu: CreditCard_DWH
-- ====================================================================================================

USE CreditCard_DWH;
GO

-- 1. Inmon Roll-up: Cuộn Doanh số & Tỷ lệ Gian lận từ Tháng lên Quý
SELECT 
    year,
    quarter,
    COUNT(DISTINCT merchant_sk) AS active_merchants,
    SUM(total_txn) AS quarterly_txns,
    ROUND(SUM(total_volume), 2) AS quarterly_volume,
    SUM(fraud_count) AS quarterly_frauds,
    ROUND(SUM(total_volume) / NULLIF(SUM(total_txn), 0), 2) AS avg_txn_amount,
    ROUND(CAST(SUM(fraud_count) AS FLOAT) * 100.0 / NULLIF(SUM(total_txn), 0), 4) AS fraud_rate_pct
FROM agg_monthly_merchant_vol
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC;
GO

-- 2. Drill-down: Phân tích Rủi ro Gian lận theo Hình thức quẹt thẻ (Chip vs Swipe)
SELECT 
    f.use_chip,
    COUNT(*) AS total_transactions,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CAST(f.is_fraud AS INT)) AS fraud_count,
    ROUND(CAST(SUM(CAST(f.is_fraud AS INT)) AS FLOAT) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(f.amount), 2) AS avg_amount
FROM fact_transactions f
GROUP BY f.use_chip
ORDER BY fraud_rate_pct DESC;
GO

-- 3. Bằng chứng Point-in-Time Join: Phân tách giao dịch qua 2 phiên bản SCD2 của thẻ 0_0
SELECT 
    f.card_sk,
    c.card_natural_key,
    c.credit_limit,
    c.card_on_dark_web,
    c.effective_date,
    c.expiration_date,
    c.is_current,
    COUNT(*) AS so_giao_dich,
    ROUND(SUM(f.amount), 2) AS tong_tien,
    MIN(f.transaction_timestamp) AS giao_dich_som_nhat,
    MAX(f.transaction_timestamp) AS giao_dich_muon_nhat
FROM fact_transactions f
JOIN dim_cards c ON f.card_sk = c.card_sk
WHERE c.card_natural_key = '0_0'
GROUP BY f.card_sk, c.card_natural_key, c.credit_limit, c.card_on_dark_web, c.effective_date, c.expiration_date, c.is_current
ORDER BY f.card_sk;
GO

-- 4. Top 10 Thương nhân (Merchant) có doanh số lớn nhất & Tỷ lệ gian lận
SELECT TOP 10
    m.merchant_name,
    m.merchant_city,
    m.merchant_state,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CAST(f.is_fraud AS INT)) AS fraud_count,
    ROUND(CAST(SUM(CAST(f.is_fraud AS INT)) AS FLOAT) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
GROUP BY m.merchant_name, m.merchant_city, m.merchant_state
ORDER BY total_volume DESC;
GO

