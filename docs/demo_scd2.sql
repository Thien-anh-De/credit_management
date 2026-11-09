-- ====================================================================================================
-- 💳 DEMO THỦ CÔNG: SCD TYPE 2 & POINT-IN-TIME JOIN TRONG SQL SERVER (SSMS)
-- Dự án: Credit Management & Fraud Monitoring Data Warehouse
-- Cơ sở dữ liệu: CreditCard_DWH
-- ====================================================================================================
-- 
-- 📌 HƯỚNG DẪN CÁCH CHẠY DEMO TRƯỚC MẶT HỘI ĐỒNG GIẢNG VIÊN:
-- 
-- 1. Mở SQL Server Management Studio (SSMS), kết nối vào SQL Server và mở file này.
-- 2. Đảm bảo góc trên bên trái SSMS đang chọn Database: [CreditCard_DWH].
-- 3. KHÔNG BẤM F5 TOÀN BỘ FILE MỘT LÚC!
--    👉 Thao tác đúng: Bôi đen (Highlight) từng KHỐI một -> Bấm F5 (Execute) -> Giải thích cho Thầy/Cô.
-- 
-- 🎯 KỊCH BẢN NGHIỆP VỤ (NÓI VỚI THẦY/CÔ):
--    "Thưa Thầy/Cô, thẻ tín dụng 0_0 của khách hàng 0 mở năm 2002 với hạn mức ban đầu là $24,295.
--     Đến ngày 2015-06-01, ngân hàng đánh giá uy tín và duyệt nâng hạn mức lên $25,000.
--     Nếu dùng SCD Type 1 (ghi đè), các giao dịch từ 2002-2015 sẽ bị gán nhầm vào hạn mức mới.
--     Nhóm áp dụng SCD Type 2 để bảo toàn lịch sử và dùng Point-in-Time Join để chia tách giao dịch."
-- ====================================================================================================

USE CreditCard_DWH;
GO


-- ====================================================================================================
-- 📍 KHỐI 1: XEM TRẠNG THÁI BAN ĐẦU CỦA THẺ 0_0 (TRƯỚC KHI BIẾN ĐỘNG)
-- 👉 Thao tác: Bôi đen 4 dòng dưới đây và bấm F5
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
WHERE card_natural_key = '0_0';

-- 💬 Thuyết minh:
-- "Thẻ 0_0 hiện chỉ có duy nhất 1 bản ghi (card_sk = 1), hạn mức $24,295, is_current = 1 (đang hiệu lực)."



-- ====================================================================================================
-- 📍 KHỐI 2: THỰC HIỆN BIẾN ĐỘNG SCD TYPE 2 (ĐÓNG DÒNG CŨ + THÊM DÒNG MỚI)
-- 👉 Kịch bản: Ngày 2015-06-01, nâng hạn mức lên $25,000 (dùng bảng tạm #t để copy nguyên xi 17 cột)
-- 👉 Thao tác: Bôi đen toàn bộ Khối 2 và bấm F5
-- ====================================================================================================

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
