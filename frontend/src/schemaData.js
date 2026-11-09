export const DEFAULT_SCHEMA_METADATA = [
  {
    table_name: "fact_transactions",
    category: "Fact",
    color: "#EF4444",
    description: "Bảng sự kiện giao dịch quẹt thẻ Atomic Grain (1 dòng = 1 lượt quẹt)",
    columns: [
      { name: "transaction_id", type: "VARCHAR(32)", key: "PK", desc: "Khóa suy biến băm MD5" },
      { name: "date_sk", type: "INT", key: "FK", ref: "dim_date.date_sk" },
      { name: "user_sk", type: "BIGINT", key: "FK", ref: "dim_users.user_sk (PIT)" },
      { name: "card_sk", type: "BIGINT", key: "FK", ref: "dim_cards.card_sk (PIT)" },
      { name: "merchant_sk", type: "BIGINT", key: "FK", ref: "dim_merchant.merchant_sk" },
      { name: "amount", type: "FLOAT", key: "", desc: "Số tiền quẹt thẻ ($)" },
      { name: "is_fraud", type: "BIT", key: "", desc: "1: Gian lận, 0: Hợp lệ" },
      { name: "use_chip", type: "VARCHAR(30)", key: "", desc: "Swipe / Chip / Online" },
      { name: "transaction_timestamp", type: "DATETIME", key: "", desc: "Thời điểm POS" }
    ]
  },
  {
    table_name: "dim_date",
    category: "Dimension",
    color: "#A855F7",
    description: "Chiều thời gian 1995–2035 (SCD Type 0)",
    columns: [
      { name: "date_sk", type: "INT", key: "PK", desc: "Khóa ngày YYYYMMDD" },
      { name: "full_date_str", type: "VARCHAR(10)", key: "", desc: "YYYY-MM-DD" },
      { name: "year", type: "INT", key: "", desc: "1995 - 2035" },
      { name: "quarter", type: "INT", key: "", desc: "Quý 1 - 4" },
      { name: "month_number", type: "INT", key: "", desc: "Tháng 1 - 12" },
      { name: "month_name", type: "VARCHAR(15)", key: "", desc: "Tên tháng" },
      { name: "day_of_month", type: "INT", key: "", desc: "Ngày 1 - 31" },
      { name: "day_of_week", type: "INT", key: "", desc: "1 (CN) - 7 (Thứ 7)" },
      { name: "day_name", type: "VARCHAR(15)", key: "", desc: "Monday, Tuesday..." },
      { name: "week_of_year", type: "INT", key: "", desc: "Tuần 1 - 53" },
      { name: "is_weekend", type: "BIT", key: "", desc: "1: Cuối tuần, 0: Thường" },
      { name: "is_holiday_vn", type: "BIT", key: "", desc: "1: Ngày nghỉ lễ, 0: Thường" }
    ]
  },
  {
    table_name: "dim_users",
    category: "Dimension",
    color: "#06B6D4",
    description: "Chiều thông tin khách hàng (SCD Type 2)",
    columns: [
      { name: "user_sk", type: "BIGINT", key: "PK", desc: "Surrogate Key phiên bản" },
      { name: "user_id", type: "BIGINT", key: "NK", desc: "Mã tự nhiên khách hàng" },
      { name: "user_name", type: "VARCHAR(100)", key: "", desc: "Họ và tên" },
      { name: "current_age", type: "INT", key: "", desc: "Tuổi hiện tại" },
      { name: "yearly_income", type: "FLOAT", key: "", desc: "Thu nhập năm ($)" },
      { name: "total_debt", type: "FLOAT", key: "", desc: "Tổng nợ ($)" },
      { name: "fico_score", type: "INT", key: "", desc: "Điểm tín dụng FICO" },
      { name: "city", type: "VARCHAR(100)", key: "", desc: "Thành phố" },
      { name: "state", type: "VARCHAR(50)", key: "", desc: "Bang / Tỉnh" },
      { name: "effective_date", type: "DATE", key: "PIT", desc: "Hiệu lực từ ngày" },
      { name: "expiration_date", type: "DATE", key: "PIT", desc: "Hết hiệu lực ngày" },
      { name: "is_current", type: "BIT", key: "", desc: "1: Bản ghi hiện hành" }
    ]
  },
  {
    table_name: "dim_cards",
    category: "Dimension",
    color: "#F59E0B",
    description: "Chiều thẻ tín dụng & rò rỉ Dark Web (SCD Type 2)",
    columns: [
      { name: "card_sk", type: "BIGINT", key: "PK", desc: "Surrogate Key phiên bản thẻ" },
      { name: "card_natural_key", type: "VARCHAR(50)", key: "NK", desc: "user_id + card_index" },
      { name: "user_id", type: "BIGINT", key: "FK", ref: "dim_users.user_id" },
      { name: "card_index", type: "INT", key: "", desc: "Thứ tự thẻ" },
      { name: "card_brand", type: "VARCHAR(50)", key: "", desc: "Visa / Mastercard / Amex" },
      { name: "card_type", type: "VARCHAR(50)", key: "", desc: "Credit / Debit / Prepaid" },
      { name: "credit_limit", type: "FLOAT", key: "", desc: "Hạn mức tín dụng ($)" },
      { name: "card_on_dark_web", type: "BIT", key: "", desc: "1: Cảnh báo lộ Dark Web" },
      { name: "acct_open_date", type: "DATE", key: "", desc: "Ngày mở thẻ" },
      { name: "effective_date", type: "DATE", key: "PIT", desc: "Bắt đầu hiệu lực" },
      { name: "expiration_date", type: "DATE", key: "PIT", desc: "Kết thúc hiệu lực" },
      { name: "is_current", type: "BIT", key: "", desc: "1: Thẻ đang dùng" }
    ]
  },
  {
    table_name: "dim_merchant",
    category: "Dimension",
    color: "#10B981",
    description: "Chiều điểm bán POS & Thương nhân (SCD Type 1)",
    columns: [
      { name: "merchant_sk", type: "BIGINT", key: "PK", desc: "Surrogate Key POS" },
      { name: "merchant_name", type: "VARCHAR(100)", key: "", desc: "Tên điểm bán / Cửa hàng" },
      { name: "merchant_city", type: "VARCHAR(100)", key: "", desc: "Thành phố đặt POS" },
      { name: "merchant_state", type: "VARCHAR(50)", key: "", desc: "Bang / Khu vực" },
      { name: "mcc", type: "VARCHAR(10)", key: "", desc: "Mã ngành hàng (MCC)" }
    ]
  },
  {
    table_name: "agg_daily_merchant_vol",
    category: "Summary",
    color: "#EC4899",
    description: "Tổng hợp doanh số Ngày x Merchant (Inmon Tầng Cơ Sở)",
    columns: [
      { name: "date_sk", type: "INT", key: "PK,FK", ref: "dim_date.date_sk" },
      { name: "merchant_sk", type: "BIGINT", key: "PK,FK", ref: "dim_merchant.merchant_sk" },
      { name: "total_txn", type: "INT", key: "", desc: "Tổng lượt quẹt" },
      { name: "total_volume", type: "FLOAT", key: "", desc: "Tổng doanh số ($)" },
      { name: "fraud_count", type: "INT", key: "", desc: "Số vụ gian lận" },
      { name: "avg_txn_amount", type: "FLOAT", key: "", desc: "Giá trị trung bình/GD" },
      { name: "fraud_rate_pct", type: "FLOAT", key: "", desc: "Tỷ lệ gian lận (%)" }
    ]
  },
  {
    table_name: "agg_monthly_merchant_vol",
    category: "Summary",
    color: "#EC4899",
    description: "Tổng hợp doanh số Tháng x Merchant (Inmon Tầng Giữa)",
    columns: [
      { name: "year", type: "INT", key: "PK", desc: "Năm tổng hợp" },
      { name: "month_number", type: "INT", key: "PK", desc: "Tháng 1 - 12" },
      { name: "merchant_sk", type: "BIGINT", key: "PK,FK", ref: "dim_merchant.merchant_sk" },
      { name: "total_txn", type: "INT", key: "", desc: "Tổng lượt quẹt tháng" },
      { name: "total_volume", type: "FLOAT", key: "", desc: "Tổng doanh số tháng ($)" },
      { name: "fraud_count", type: "INT", key: "", desc: "Số vụ gian lận tháng" },
      { name: "avg_txn_amount", type: "FLOAT", key: "", desc: "Giá trị trung bình/GD" },
      { name: "fraud_rate_pct", type: "FLOAT", key: "", desc: "Tỷ lệ gian lận (%)" }
    ]
  },
  {
    table_name: "agg_quarterly_merchant_vol",
    category: "Summary",
    color: "#EC4899",
    description: "Tổng hợp doanh số Quý x Merchant (Inmon Đỉnh Tháp)",
    columns: [
      { name: "year", type: "INT", key: "PK", desc: "Năm tổng hợp" },
      { name: "quarter", type: "INT", key: "PK", desc: "Quý 1 - 4" },
      { name: "merchant_sk", type: "BIGINT", key: "PK,FK", ref: "dim_merchant.merchant_sk" },
      { name: "total_txn", type: "INT", key: "", desc: "Tổng lượt quẹt quý" },
      { name: "total_volume", type: "FLOAT", key: "", desc: "Tổng doanh số quý ($)" },
      { name: "fraud_count", type: "INT", key: "", desc: "Số vụ gian lận quý" },
      { name: "avg_txn_amount", type: "FLOAT", key: "", desc: "Giá trị trung bình/GD" },
      { name: "fraud_rate_pct", type: "FLOAT", key: "", desc: "Tỷ lệ gian lận (%)" }
    ]
  },
  {
    table_name: "agg_daily_card_fraud",
    category: "Summary",
    color: "#F43F5E",
    description: "Giám sát rủi ro thẻ Ngày x Thẻ (Risk Branch)",
    columns: [
      { name: "date_sk", type: "INT", key: "PK,FK", ref: "dim_date.date_sk" },
      { name: "card_sk", type: "BIGINT", key: "PK,FK", ref: "dim_cards.card_sk" },
      { name: "total_txn", type: "INT", key: "", desc: "Lượt quẹt trong ngày" },
      { name: "total_amount", type: "FLOAT", key: "", desc: "Tổng tiền quẹt ($)" },
      { name: "fraud_count", type: "INT", key: "", desc: "Số vụ gian lận" },
      { name: "avg_amount", type: "FLOAT", key: "", desc: "Giao dịch trung bình" },
      { name: "max_amount", type: "FLOAT", key: "", desc: "Giao dịch lớn nhất" },
      { name: "fraud_rate_pct", type: "FLOAT", key: "", desc: "Tỷ lệ gian lận (%)" }
    ]
  }
];

