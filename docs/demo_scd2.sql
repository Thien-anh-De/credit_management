
USE CreditCard_DWH;
GO


-- XEM TRẠNG THÁI BAN ĐẦU CỦA THẺ 0_0 (TRƯỚC KHI BIẾN ĐỘNG)
    card_sk,
    card_natural_key,
    credit_limit,
    card_on_dark_web,
    effective_date,
    expiration_date,
    is_current
FROM dim_cards
WHERE card_natural_key = '0_0';


-- THỰC HIỆN BIẾN ĐỘNG SCD TYPE 2 (ĐÓNG DÒNG CŨ + THÊM DÒNG MỚI)
-- Bước A: Copy toàn bộ thông tin thẻ sang bảng tạm #t
IF OBJECT_ID('tempdb..#t') IS NOT NULL DROP TABLE #t;
SELECT * INTO #t FROM dim_cards WHERE card_sk = 1;

-- Bước B: Sửa hạn mức mới ($25,000) và ngày hiệu lực mới (2015-06-01) trên bảng tạm
UPDATE #t 
SET card_sk = 6147,          -- Surrogate Key mới
    credit_limit = 25000.0,  -- Hạn mức mới
    effective_date = '2015-06-01',
    expiration_date = '9999-12-31',
    is_current = 1;

-- Bước C: Đóng phiên bản cũ trong bảng chính (hết hạn ngày 2015-06-01, is_current = 0)
UPDATE dim_cards 
SET expiration_date = '2015-06-01', 
    is_current = 0 
WHERE card_sk = 1;

-- Bước D: Chèn phiên bản mới vào bảng chính và xóa bảng tạm
INSERT INTO dim_cards SELECT * FROM #t;
DROP TABLE #t;

-- 💬 Thuyết minh:
-- "Hệ thống đã đóng phiên bản cũ với expiration_date = 2015-06-01 và mở phiên bản mới card_sk = 6147."



-- ====================================================================================================
-- 📍 KHỐI 3: KIỂM TRA BẢNG CHIỀU dim_cards SAU KHI CẬP NHẬT
-- 👉 Thao tác: Bôi đen khối này và bấm F5
-- ====================================================================================================
SELECT 
    card_sk,
    card_natural_key,
    credit_limit,
    card_on_dark_web,
    effective_date,
    expiration_date,
    is_current
FROM dim_cards
WHERE card_natural_key = '0_0'
ORDER BY card_sk;

-- 💬 Thuyết minh:
-- "Thầy/Cô thấy rõ thẻ 0_0 xuất hiện 2 phiên bản nối tiếp nhau liên tục, không chồng lấn:
--  - Phiên bản 1 (card_sk = 1): Hạn mức cũ $24,295, hiệu lực từ 2002 đến 2015-06-01 (is_current = 0).
--  - Phiên bản 2 (card_sk = 6147): Hạn mức mới $25,000, hiệu lực từ 2015-06-01 đến 9999 (is_current = 1)."



-- ====================================================================================================
-- 📍 KHỐI 4: BẰNG CHỨNG POINT-IN-TIME JOIN TRÊN BẢNG FACT (fact_transactions)
-- 👉 Thao tác: Bôi đen khối này và bấm F5
-- ====================================================================================================

-- Cập nhật giao dịch sau mốc 2015-06-01 gắn vào card_sk mới
UPDATE fact_transactions 
SET card_sk = 6147 
WHERE card_sk = 1 
  AND transaction_timestamp >= '2015-06-01';

-- XEM BẰNG CHỨNG PHÂN TÁCH GIAO DỊCH QUA 2 PHIÊN BẢN:
SELECT 
    f.card_sk,
    c.credit_limit,
    c.effective_date,
    c.expiration_date,
    c.is_current,
    COUNT(*) AS so_luong_giao_dich,
    ROUND(SUM(f.amount), 2) AS tong_tien_quyet,
    MIN(f.transaction_timestamp) AS giao_dich_som_nhat,
    MAX(f.transaction_timestamp) AS giao_dich_muon_nhat
FROM fact_transactions f
JOIN dim_cards c ON f.card_sk = c.card_sk
WHERE c.card_natural_key = '0_0'
GROUP BY f.card_sk, c.credit_limit, c.effective_date, c.expiration_date, c.is_current
ORDER BY f.card_sk;

-- 💬 Thuyết minh:
-- "Thưa Thầy/Cô, đây là bằng chứng Point-in-Time Join:
--  - Các giao dịch trước ngày 2015-06-01 gắn chính xác với card_sk = 1 (hạn mức cũ).
--  - Các giao dịch từ ngày 2015-06-01 trở đi gắn với card_sk = 6147 (hạn mức mới).
--  Báo cáo tài chính quá khứ được bảo toàn 100% không bị sai lệch!"



-- ====================================================================================================
-- 📍 KHỐI 5: HOÀN TÁC (RESET) ĐỂ ĐƯA DATABASE VỀ NGUYÊN TRẠNG SAU KHI DEMO
-- 👉 Thao tác: Bôi đen khối này và bấm F5 khi muốn trả lại dữ liệu ban đầu
-- ====================================================================================================
UPDATE fact_transactions SET card_sk = 1 WHERE card_sk = 6147;
DELETE FROM agg_daily_card_fraud WHERE card_sk = 6147;
DELETE FROM dim_cards WHERE card_sk = 6147;
UPDATE dim_cards SET expiration_date = '9999-12-31', is_current = 1 WHERE card_sk = 1;

-- Kiểm tra lại để xác nhận đã khôi phục (chỉ còn đúng 1 bản ghi ban đầu):
SELECT card_sk, card_natural_key, credit_limit, effective_date, expiration_date, is_current 
FROM dim_cards 
WHERE card_natural_key = '0_0';
