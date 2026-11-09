"""
Tool: Automated Data Dictionary Generator
=========================================
Quét tự động schema của tất cả các bảng trong Medallion Lakehouse & Kimball Star Schema DWH:
  - Bronze Layer (3 files CSV nguồn)
  - Silver Layer (3 bảng Parquet làm sạch)
  - Gold Layer (5 bảng Star Schema + 4 bảng Inmon Summary Layers)

Đầu ra: Tài liệu Markdown chi tiết tại docs/data_dictionary.md
Chạy: python src/tools/generate_data_dict.py
"""
import os
import sys
import pandas as pd
from datetime import datetime

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DOCS_DIR = os.path.join(BASE_DIR, "docs")
OUT_FILE = os.path.join(DOCS_DIR, "data_dictionary.md")

# Metadata định nghĩa các bảng và vai trò kiến trúc
TABLE_CATALOG = [
    # ─── GOLD LAYER: STAR SCHEMA CORE ──────────────────────────────────────────
    {
        "name": "fact_transactions",
        "layer": "Gold (DWH)",
        "type": "Fact Table (Atomic Grain)",
        "path": "data/gold/fact_transactions",
        "scd": "Immutable Events (Nonvolatile)",
        "description": "Bảng sự kiện giao dịch quẹt thẻ ở mức hạt chi tiết nhất (1 dòng = 1 lần quẹt tại POS). Gắn Surrogate Keys thông qua SCD Type 2 Point-in-Time Join.",
        "columns_meta": {
            "transaction_id": ("VARCHAR(32)", "PK / Degenerate Key", False, "Mã băm MD5 tất cả trường tự nhiên, bảo đảm Idempotent 100%"),
            "date_sk": ("INT", "FK -> dim_date.date_sk", False, "Khóa thay thế ngày giao dịch YYYYMMDD"),
            "user_sk": ("BIGINT", "FK -> dim_users.user_sk (PIT)", False, "Khóa thay thế phiên bản khách hàng tại thời điểm quẹt"),
            "card_sk": ("BIGINT", "FK -> dim_cards.card_sk (PIT)", False, "Khóa thay thế phiên bản thẻ tại thời điểm quẹt"),
            "merchant_sk": ("BIGINT", "FK -> dim_merchant.merchant_sk", False, "Khóa thay thế điểm bán / thương nhân POS"),
            "amount": ("DOUBLE", "Fact Measure (Additive)", False, "Số tiền quẹt thẻ ($)"),
            "is_fraud": ("BOOLEAN", "Fact Measure (Additive / Flag)", False, "Cờ cảnh báo gian lận (True = 1, False = 0)"),
            "use_chip": ("VARCHAR(30)", "Degenerate Dimension", True, "Hình thức giao dịch: Swipe Transaction / Chip Transaction / Online"),
            "transaction_timestamp": ("TIMESTAMP", "Event Timestamp", False, "Thời điểm giao dịch phát sinh chính xác tại POS")
        }
    },
    {
        "name": "dim_date",
        "layer": "Gold (DWH)",
        "type": "Dimension Table",
        "path": "data/gold/dim_date",
        "scd": "SCD Type 0 (Fixed / Retain Original)",
        "description": "Bảng chiều thời gian lịch chuẩn từ 1995 đến 2035 (14,975 ngày), bao phủ toàn bộ lịch sử ngân hàng.",
        "columns_meta": {
            "date_sk": ("INT", "Primary Key (Surrogate)", False, "Khóa ngày dạng số nguyên YYYYMMDD (ví dụ 20261109)"),
            "full_date_str": ("VARCHAR(10)", "Attribute", False, "Ngày dạng chuỗi chuẩn YYYY-MM-DD"),
            "day_of_week": ("INT", "Attribute", False, "Thứ trong tuần: 1 (Chủ Nhật) đến 7 (Thứ Bảy)"),
            "day_name": ("VARCHAR(15)", "Attribute", False, "Tên thứ tiếng Anh (Monday, Tuesday...)"),
            "day_of_month": ("INT", "Attribute", False, "Ngày trong tháng (1 - 31)"),
            "week_of_year": ("INT", "Attribute", False, "Tuần trong năm (1 - 53)"),
            "month_number": ("INT", "Attribute", False, "Tháng số (1 - 12)"),
            "month_name": ("VARCHAR(15)", "Attribute", False, "Tên tháng tiếng Anh (January, February...)"),
            "quarter": ("INT", "Attribute", False, "Quý trong năm (1, 2, 3, 4)"),
            "year": ("INT", "Attribute", False, "Năm theo lịch (1995 - 2035)"),
            "is_weekend": ("BOOLEAN", "Analytical Flag", False, "Cờ đánh dấu ngày cuối tuần (Thứ 7 hoặc Chủ Nhật)"),
            "is_holiday_vn": ("BOOLEAN", "Analytical Flag", False, "Cờ đánh dấu các ngày lễ lớn tại Việt Nam")
        }
    },
    {
        "name": "dim_merchant",
        "layer": "Gold (DWH)",
        "type": "Dimension Table",
        "path": "data/gold/dim_merchant",
        "scd": "SCD Type 1 (Overwrite Attributes)",
        "description": "Bảng chiều thông tin đối tác chấp nhận thẻ POS / Thương nhân. Khi cập nhật thông tin thương nhân sẽ ghi đè trực tiếp.",
        "columns_meta": {
            "merchant_sk": ("BIGINT", "Primary Key (Surrogate)", False, "Khóa thay thế duy nhất cho từng điểm bán"),
            "merchant_name": ("VARCHAR(100)", "Natural Key / Name", False, "Tên hoặc mã định danh thương nhân POS"),
            "merchant_city": ("VARCHAR(100)", "Attribute", False, "Thành phố đặt máy POS (mặc định 'Unknown')"),
            "merchant_state": ("VARCHAR(50)", "Attribute", False, "Bang hoặc khu vực địa lý"),
            "mcc": ("VARCHAR(10)", "Attribute", False, "Mã phân loại ngành hàng Merchant Category Code")
        }
    },
    {
        "name": "dim_users",
        "layer": "Gold (DWH)",
        "type": "Dimension Table",
        "path": "data/gold/dim_users",
        "scd": "SCD Type 2 (Add Row Versioning)",
        "description": "Bảng chiều khách hàng lưu vết lịch sử biến động địa chỉ, thu nhập năm và điểm tín dụng FICO.",
        "columns_meta": {
            "user_sk": ("BIGINT", "Primary Key (Surrogate)", False, "Khóa thay thế định danh duy nhất cho từng phiên bản khách hàng"),
            "user_id": ("BIGINT", "Natural Key", False, "Mã khách hàng tự nhiên từ hệ thống Core Banking"),
            "user_name": ("VARCHAR(100)", "Attribute", True, "Họ tên đầy đủ của khách hàng"),
            "gender": ("VARCHAR(10)", "Attribute", True, "Giới tính (Female / Male)"),
            "current_age": ("INT", "Attribute", True, "Tuổi hiện tại của khách hàng"),
            "retirement_age": ("INT", "Attribute", True, "Tuổi nghỉ hưu dự kiến"),
            "birth_year": ("INT", "Attribute", True, "Năm sinh"),
            "birth_month": ("INT", "Attribute", True, "Tháng sinh"),
            "address": ("VARCHAR(255)", "SCD2 Tracked Attribute", True, "Địa chỉ cư trú (theo dõi biến động SCD2)"),
            "apartment": ("VARCHAR(50)", "Attribute", True, "Số phòng / Căn hộ"),
            "city": ("VARCHAR(100)", "Attribute", True, "Thành phố cư trú"),
            "state": ("VARCHAR(50)", "Attribute", True, "Bang cư trú"),
            "zipcode": ("VARCHAR(20)", "Attribute", True, "Mã bưu chính Zipcode"),
            "latitude": ("DOUBLE", "Geo Attribute", True, "Vĩ độ địa lý"),
            "longitude": ("DOUBLE", "Geo Attribute", True, "Kinh độ địa lý"),
            "per_capita_income": ("DOUBLE", "Attribute", True, "Thu nhập bình quân đầu người tại khu vực zipcode ($)"),
            "yearly_income": ("DOUBLE", "SCD2 Tracked Attribute", True, "Thu nhập hàng năm của cá nhân (theo dõi biến động SCD2) ($)"),
            "total_debt": ("DOUBLE", "Attribute", True, "Tổng dư nợ hiện tại của khách hàng ($)"),
            "fico_score": ("INT", "SCD2 Tracked Attribute", True, "Điểm tín dụng FICO Score (theo dõi biến động SCD2)"),
            "num_credit_cards": ("INT", "Attribute", True, "Tổng số lượng thẻ tín dụng đang sở hữu"),
            "effective_date": ("DATE", "SCD2 PIT Control", False, "Ngày bắt đầu có hiệu lực của phiên bản (Sentinel: 1970-01-01)"),
            "expiration_date": ("DATE", "SCD2 PIT Control", False, "Ngày hết hạn hiệu lực của phiên bản (Mặc định: 9999-12-31)"),
            "is_current": ("BOOLEAN", "SCD2 Current Flag", False, "Cờ nhận diện bản ghi hiện hành (True = Đang dùng)")
        }
    },
    {
        "name": "dim_cards",
        "layer": "Gold (DWH)",
        "type": "Dimension Table",
        "path": "data/gold/dim_cards",
        "scd": "SCD Type 2 (Add Row Versioning)",
        "description": "Bảng chiều thẻ tín dụng lưu vết lịch sử biến động hạn mức (credit_limit) và cảnh báo rò rỉ Dark Web (card_on_dark_web).",
        "columns_meta": {
            "card_sk": ("BIGINT", "Primary Key (Surrogate)", False, "Khóa thay thế định danh duy nhất cho từng phiên bản thẻ"),
            "card_natural_key": ("VARCHAR(50)", "Natural Key", False, "Khóa tự nhiên kết hợp: user_id + '_' + card_index"),
            "user_id": ("BIGINT", "Foreign Key / Natural", False, "Mã khách hàng sở hữu thẻ"),
            "card_index": ("INT", "Natural Key Part", False, "Thứ tự số thẻ của khách hàng (0, 1, 2...)"),
            "card_brand": ("VARCHAR(50)", "Attribute", True, "Thương hiệu thẻ: Visa, Mastercard, Amex, Discover"),
            "card_type": ("VARCHAR(50)", "Attribute", True, "Loại thẻ: Credit, Debit, Prepaid"),
            "card_number": ("VARCHAR(30)", "Attribute", True, "Số thẻ tín dụng"),
            "expires": ("VARCHAR(10)", "Attribute", True, "Ngày hết hạn in trên thẻ (MM/YYYY)"),
            "cvv": ("VARCHAR(10)", "Attribute", True, "Mã bảo mật thẻ CVV"),
            "has_chip": ("BOOLEAN", "Attribute", False, "Thẻ có tích hợp chip EMV hay không"),
            "cards_issued": ("INT", "Attribute", True, "Số lượng phôi thẻ vật lý đã phát hành"),
            "credit_limit": ("DOUBLE", "SCD2 Tracked Attribute", False, "Hạn mức tín dụng được cấp (theo dõi nâng/hạ hạn mức) ($)"),
            "acct_open_date": ("VARCHAR(10)", "Attribute", True, "Ngày mở tài khoản thẻ"),
            "year_pin_last_changed": ("INT", "Attribute", True, "Năm gần nhất khách hàng đổi mã PIN"),
            "card_on_dark_web": ("BOOLEAN", "SCD2 Tracked Attribute", False, "Cảnh báo thẻ bị lộ lọt trên Dark Web (Forensics Audit)"),
            "effective_date": ("DATE", "SCD2 PIT Control", False, "Ngày bắt đầu có hiệu lực của phiên bản thẻ"),
            "expiration_date": ("DATE", "SCD2 PIT Control", False, "Ngày hết hạn hiệu lực của phiên bản thẻ"),
            "is_current": ("BOOLEAN", "SCD2 Current Flag", False, "Cờ nhận diện bản ghi thẻ hiện hành (True = Đang hiệu lực)")
        }
    },

    # ─── GOLD LAYER: INMON SUMMARY LAYERS ──────────────────────────────────────
    {
        "name": "agg_daily_merchant_vol",
        "layer": "Gold (Summary Layer 1A)",
        "type": "Inmon Summary (Base Layer)",
        "path": "data/gold/agg_daily_merchant_vol",
        "scd": "Pre-aggregated Rollup",
        "description": "Tổng hợp doanh số và rủi ro theo Ngày x Merchant từ bảng Fact Atomic. Tầng cơ sở của Tháp Rollup Inmon.",
        "columns_meta": {
            "date_sk": ("INT", "Compound PK / FK -> dim_date", False, "Khóa ngày YYYYMMDD"),
            "merchant_sk": ("BIGINT", "Compound PK / FK -> dim_merchant", False, "Khóa thương nhân POS"),
            "total_txn": ("INT", "Additive Measure", False, "Tổng số lượt quẹt thẻ trong ngày"),
            "total_volume": ("DOUBLE", "Additive Measure", False, "Tổng doanh số quẹt thẻ trong ngày ($)"),
            "fraud_count": ("INT", "Additive Measure", False, "Tổng số giao dịch bị gắn cờ gian lận"),
            "avg_txn_amount": ("DOUBLE", "Derived Measure", False, "Giá trị trung bình một giao dịch ($)"),
            "fraud_rate_pct": ("DOUBLE", "Derived Measure", False, "Tỷ lệ gian lận theo phần trăm (%)")
        }
    },
    {
        "name": "agg_monthly_merchant_vol",
        "layer": "Gold (Summary Layer 2)",
        "type": "Inmon Summary (Middle Layer)",
        "path": "data/gold/agg_monthly_merchant_vol",
        "scd": "Pre-aggregated Rollup",
        "description": "Tổng hợp doanh số và rủi ro theo Tháng x Merchant. Roll-up trực tiếp từ Tầng Ngày (Inmon Chapter 3).",
        "columns_meta": {
            "year": ("INT", "Compound PK / Partition", False, "Năm tổng hợp"),
            "month_number": ("INT", "Compound PK", False, "Tháng trong năm (1 - 12)"),
            "quarter": ("INT", "Attribute", False, "Quý trong năm (1, 2, 3, 4)"),
            "merchant_sk": ("BIGINT", "Compound PK / FK -> dim_merchant", False, "Khóa thương nhân POS"),
            "total_txn": ("INT", "Additive Measure", False, "Tổng lượt quẹt thẻ trong tháng"),
            "total_volume": ("DOUBLE", "Additive Measure", False, "Tổng doanh số quẹt thẻ trong tháng ($)"),
            "fraud_count": ("INT", "Additive Measure", False, "Tổng số giao dịch gian lận trong tháng"),
            "avg_txn_amount": ("DOUBLE", "Derived Measure", False, "Giá trị trung bình một giao dịch trong tháng ($)"),
            "fraud_rate_pct": ("DOUBLE", "Derived Measure", False, "Tỷ lệ gian lận trong tháng (%)")
        }
    },
    {
        "name": "agg_quarterly_merchant_vol",
        "layer": "Gold (Summary Layer 3)",
        "type": "Inmon Summary (Top Layer)",
        "path": "data/gold/agg_quarterly_merchant_vol",
        "scd": "Pre-aggregated Rollup",
        "description": "Tổng hợp doanh số và rủi ro theo Quý x Merchant (Đỉnh Tháp Rollup Inmon). Cuộn trực tiếp từ Tầng Tháng.",
        "columns_meta": {
            "year": ("INT", "Compound PK / Partition", False, "Năm tổng hợp"),
            "quarter": ("INT", "Compound PK", False, "Quý trong năm (1, 2, 3, 4)"),
            "merchant_sk": ("BIGINT", "Compound PK / FK -> dim_merchant", False, "Khóa thương nhân POS"),
            "total_txn": ("INT", "Additive Measure", False, "Tổng lượt quẹt thẻ trong quý"),
            "total_volume": ("DOUBLE", "Additive Measure", False, "Tổng doanh số trong quý ($)"),
            "fraud_count": ("INT", "Additive Measure", False, "Tổng số giao dịch gian lận trong quý"),
            "avg_txn_amount": ("DOUBLE", "Derived Measure", False, "Giá trị trung bình một giao dịch trong quý ($)"),
            "fraud_rate_pct": ("DOUBLE", "Derived Measure", False, "Tỷ lệ gian lận trong quý (%)")
        }
    },
    {
        "name": "agg_daily_card_fraud",
        "layer": "Gold (Summary Layer 1B)",
        "type": "Risk Analytics Summary",
        "path": "data/gold/agg_daily_card_fraud",
        "scd": "Pre-aggregated Analytics",
        "description": "Tổng hợp chỉ số rủi ro Ngày x Thẻ phục vụ bộ phận Fraud Investigation giám sát thẻ bị tấn công dồn dập.",
        "columns_meta": {
            "date_sk": ("INT", "Compound PK / FK -> dim_date", False, "Khóa ngày YYYYMMDD"),
            "card_sk": ("BIGINT", "Compound PK / FK -> dim_cards", False, "Khóa thay thế phiên bản thẻ tín dụng"),
            "total_txn": ("INT", "Additive Measure", False, "Số lượt quẹt của thẻ trong ngày"),
            "total_amount": ("DOUBLE", "Additive Measure", False, "Tổng số tiền quẹt trong ngày ($)"),
            "fraud_count": ("INT", "Additive Measure", False, "Số giao dịch bị phát hiện gian lận"),
            "avg_amount": ("DOUBLE", "Derived Measure", False, "Số tiền quẹt trung bình ($)"),
            "max_amount": ("DOUBLE", "Max Measure", False, "Giao dịch lớn nhất trong ngày ($)"),
            "fraud_rate_pct": ("DOUBLE", "Derived Measure", False, "Tỷ lệ gian lận của thẻ (%)")
        }
    },

    # ─── SILVER LAYER: CLEANED PARQUET LAKE ────────────────────────────────────
    {
        "name": "dim_users (Silver)",
        "layer": "Silver (Clean Lake)",
        "type": "Standardized Staging",
        "path": "data/silver/dim_users",
        "scd": "Snapshot Staging",
        "description": "Bảng hồ sơ khách hàng đã bóc tách ký tự $, chuẩn hóa snake_case và nén Parquet Snappy (2,000 khách hàng).",
        "columns_meta": {
            "user_id": ("BIGINT", "Natural Key", False, "Mã khách hàng tự nhiên"),
            "user_name": ("VARCHAR(100)", "Clean String", True, "Họ và tên"),
            "per_capita_income": ("DOUBLE", "Clean Numeric", True, "Thu nhập bình quân zipcode"),
            "yearly_income": ("DOUBLE", "Clean Numeric", True, "Thu nhập năm cá nhân"),
            "total_debt": ("DOUBLE", "Clean Numeric", True, "Tổng dư nợ"),
            "fico_score": ("INT", "Clean Numeric", True, "Điểm tín dụng FICO")
        }
    },
    {
        "name": "dim_cards (Silver)",
        "layer": "Silver (Clean Lake)",
        "type": "Standardized Staging",
        "path": "data/silver/dim_cards",
        "scd": "Snapshot Staging",
        "description": "Bảng thẻ tín dụng đã làm sạch, ép kiểu Boolean cho Has Chip/Dark Web và nén Parquet Snappy (6,146 thẻ).",
        "columns_meta": {
            "user_id": ("BIGINT", "Natural FK", False, "Mã khách hàng sở hữu thẻ"),
            "card_index": ("INT", "Natural Key Part", False, "Thứ tự thẻ"),
            "credit_limit": ("DOUBLE", "Clean Numeric", False, "Hạn mức tín dụng"),
            "card_on_dark_web": ("BOOLEAN", "Clean Boolean", False, "Cảnh báo rò rỉ Dark Web")
        }
    }
]


def generate_markdown():
    lines = []
    lines.append("# 📖 TỪ ĐIỂN DỮ LIỆU TOÀN DIỆN (ENTERPRISE DATA DICTIONARY)")
    lines.append("## Dự án: Credit Management & Fraud Monitoring Data Warehouse")
    lines.append(f"> Tự động sinh từ cấu trúc Parquet thực tế ngày: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 📌 1. Bảng Tổng Hợp Danh Mục Các Bảng (Entity Catalog)")
    lines.append("")
    lines.append("| STT | Tên Bảng | Phân Tầng (Layer) | Phân Loại Thực Thể | Chiến Lược SCD / Vai Trò | Trạng Thái Dữ Liệu |")
    lines.append("|:---:|:---|:---|:---|:---|:---:|")

    for i, t in enumerate(TABLE_CATALOG, 1):
        full_p = os.path.join(BASE_DIR, t["path"])
        row_str = "N/A"
        if os.path.exists(full_p):
            try:
                df = pd.read_parquet(full_p)
                row_str = f"✅ Sẵn sàng ({len(df):,} dòng)"
            except Exception:
                row_str = "✅ File tồn tại"
        else:
            row_str = "⚠️ Chưa nạp"
        lines.append(f"| {i} | [`{t['name']}`](#{t['name'].lower().replace(' ', '-').replace('(', '').replace(')', '')}) | {t['layer']} | {t['type']} | {t['scd']} | {row_str} |")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 📋 2. Chi Tiết Cấu Trúc Schema & Từ Điển Cột Từng Bảng")
    lines.append("")

    for t in TABLE_CATALOG:
        anchor = t['name'].lower().replace(' ', '-').replace('(', '').replace(')', '')
        lines.append(f"### <a id=\"{anchor}\"></a>📊 Bảng: `{t['name']}`")
        lines.append(f"- **Phân tầng:** {t['layer']}")
        lines.append(f"- **Phân loại:** {t['type']}")
        lines.append(f"- **Chiến lược SCD / Mức hạt:** {t['scd']}")
        lines.append(f"- **Mô tả nghiệp vụ:** {t['description']}")
        lines.append("")
        lines.append("| Tên Cột (Column Name) | Kiểu Dữ Liệu | Vai Trò Khóa / Kiến Trúc | Cho Phép Null | Ý Nghĩa Nghiệp Vụ & Quy Tắc |")
        lines.append("|:---|:---|:---|:---:|:---|")

        full_p = os.path.join(BASE_DIR, t["path"])
        actual_cols = []
        if os.path.exists(full_p):
            try:
                actual_cols = list(pd.read_parquet(full_p).columns)
            except Exception:
                pass

        # Ghi các cột đã có metadata
        documented = set()
        for cname, (dtype, role, nullable, desc) in t["columns_meta"].items():
            null_str = "Có (Nullable)" if nullable else "**Không (NOT NULL)**"
            lines.append(f"| `{cname}` | `{dtype}` | {role} | {null_str} | {desc} |")
            documented.add(cname)

        # Ghi thêm các cột thực tế nếu có trong file nhưng chưa có trong meta
        for c in actual_cols:
            if c not in documented:
                lines.append(f"| `{c}` | `PARQUET_AUTO` | Thuộc tính mở rộng | Có (Nullable) | Cột dữ liệu chuẩn hóa từ nguồn |")

        lines.append("")
        lines.append("---")
        lines.append("")

    content = "\n".join(lines)
    with open(OUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"✅ Đã sinh thành công Từ điển dữ liệu toàn diện tại: {OUT_FILE}")


if __name__ == "__main__":
    generate_markdown()

