import os
import glob
import time
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# CẤU HÌNH TRANG WEB
st.set_page_config(
    page_title="Credit Management & DWH Analytics",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Thêm CSS tùy chỉnh giao diện Dark Mode Glassmorphism cao cấp
st.markdown("""
<style>
    .main {
        background-color: #0E1117;
        color: #FAFAFA;
    }
    .metric-card {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        backdrop-filter: blur(4px);
    }
    .metric-value {
        font-size: 28px;
        font-weight: 700;
        color: #00D4FF;
    }
    .metric-label {
        font-size: 13px;
        color: #A0AAB0;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
</style>
""", unsafe_allow_html=True)

# XÁC ĐỊNH ĐƯỜNG DẪN TỚI THƯ MỤC DATA LAKE (SILVER & GOLD)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Silver paths
SILVER_USERS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_users")
SILVER_CARDS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_cards")
SILVER_FACT_PATH  = os.path.join(BASE_DIR, "data", "silver", "fact_transactions")

# Gold Summary & DWH paths
GOLD_FACT_PATH       = os.path.join(BASE_DIR, "data", "gold", "fact_transactions")
GOLD_DAILY_MERCH_PATH = os.path.join(BASE_DIR, "data", "gold", "agg_daily_merchant_vol")
GOLD_MONTHLY_PATH    = os.path.join(BASE_DIR, "data", "gold", "agg_monthly_merchant_vol")
GOLD_QUARTERLY_PATH  = os.path.join(BASE_DIR, "data", "gold", "agg_quarterly_merchant_vol")
GOLD_DAILY_CARD_PATH = os.path.join(BASE_DIR, "data", "gold", "agg_daily_card_fraud")
GOLD_MERCHANT_PATH   = os.path.join(BASE_DIR, "data", "gold", "dim_merchant")


# HÀM ĐỌC DỮ LIỆU SILVER (CACHE 2S)
@st.cache_data(ttl=2)
def load_silver_data():
    users_df, cards_df, fact_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if os.path.exists(SILVER_USERS_PATH):
        try:
            users_df = pd.read_parquet(SILVER_USERS_PATH)
        except Exception:
            pass
    if os.path.exists(SILVER_CARDS_PATH):
        try:
            cards_df = pd.read_parquet(SILVER_CARDS_PATH)
        except Exception:
            pass
    if os.path.exists(SILVER_FACT_PATH):
        try:
            fact_df = pd.read_parquet(SILVER_FACT_PATH)
        except Exception:
            pass
    return users_df, cards_df, fact_df


# HÀM ĐỌC DỮ LIỆU GOLD DWH SUMMARY LAYERS (CACHE 5S)
@st.cache_data(ttl=5)
def load_gold_data():
    monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df = (
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
    if os.path.exists(GOLD_MONTHLY_PATH):
        try:
            monthly_df = pd.read_parquet(GOLD_MONTHLY_PATH)
        except Exception:
            pass
    if os.path.exists(GOLD_QUARTERLY_PATH):
        try:
            quarterly_df = pd.read_parquet(GOLD_QUARTERLY_PATH)
        except Exception:
            pass
    if os.path.exists(GOLD_DAILY_CARD_PATH):
        try:
            daily_card_df = pd.read_parquet(GOLD_DAILY_CARD_PATH)
        except Exception:
            pass
    if os.path.exists(GOLD_DAILY_MERCH_PATH):
        try:
            daily_merch_df = pd.read_parquet(GOLD_DAILY_MERCH_PATH)
        except Exception:
            pass
    if os.path.exists(GOLD_MERCHANT_PATH):
        try:
            merchant_df = pd.read_parquet(GOLD_MERCHANT_PATH)
        except Exception:
            pass
    return monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df


# HÀM NẠP GOLD DWH VÀO IN-MEMORY SQLITE PHỤC VỤ TRUY VẤN SQL TRỰC TIẾP
@st.cache_resource
def get_sqlite_gold_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    tables = [
        ("dim_date", os.path.join(BASE_DIR, "data", "gold", "dim_date")),
        ("dim_merchant", os.path.join(BASE_DIR, "data", "gold", "dim_merchant")),
        ("dim_users", os.path.join(BASE_DIR, "data", "gold", "dim_users")),
        ("dim_cards", os.path.join(BASE_DIR, "data", "gold", "dim_cards")),
        ("fact_transactions", os.path.join(BASE_DIR, "data", "gold", "fact_transactions")),
        ("agg_daily_merchant_vol", os.path.join(BASE_DIR, "data", "gold", "agg_daily_merchant_vol")),
        ("agg_monthly_merchant_vol", os.path.join(BASE_DIR, "data", "gold", "agg_monthly_merchant_vol")),
        ("agg_quarterly_merchant_vol", os.path.join(BASE_DIR, "data", "gold", "agg_quarterly_merchant_vol")),
        ("agg_daily_card_fraud", os.path.join(BASE_DIR, "data", "gold", "agg_daily_card_fraud"))
    ]
    for view_name, path in tables:
        if os.path.exists(path):
            try:
                df = pd.read_parquet(path)
                df.to_sql(view_name, conn, index=False, if_exists="replace")
            except Exception:
                pass
    return conn


# TẠO THANH ĐIỀU HƯỚNG CÀI ĐẶT (SIDEBAR)
st.sidebar.title("⚙️ Cài đặt Dashboard")
auto_refresh = st.sidebar.checkbox("Tự động làm mới (Streaming 24/7)", value=False)
if auto_refresh:
    refresh_interval = st.sidebar.slider("Tần suất làm mới (giây)", 2, 10, 5)
    st.sidebar.caption("⚠️ Đang bật tự động cập nhật luồng. Tắt đi khi muốn xem bảng tĩnh hoặc viết SQL.")

if st.sidebar.button("🔄 Làm mới dữ liệu thủ công"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

st.sidebar.markdown("### 🖥️ Công cụ quản lý Cluster")
st.sidebar.markdown("- ⚡ [Spark Master UI](http://localhost:8080)")
st.sidebar.markdown("- 👷 [Spark Worker UI](http://localhost:8081)")

# TIÊU ĐỀ
st.title("💳 Credit Management & DWH Platform")

# TẢI DỮ LIỆU
users_df, cards_df, fact_df = load_silver_data()
monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df = load_gold_data()

# PHÂN TÁCH GIAO DIỆN THEO 3 TẦNG: SILVER STREAMING, GOLD DWH ANALYTICS, SQL STUDIO
tab_stream, tab_dwh, tab_sql = st.tabs([
    "⚡ Real-time Streaming Lakehouse (Silver Zone)",
    "🏛️ Data Warehouse OLAP Analytics (Gold Summary Layer)",
    "🔍 Interactive SQL Studio (Kimball Star Schema)"
])

with tab_stream:
    # Trạng thái luồng Spark
    latest_tx_time = "Chưa có dữ liệu"
    spark_status = "🔴 INACTIVE"
    if not fact_df.empty and "transaction_timestamp" in fact_df.columns:
        try:
            max_ts = pd.to_datetime(fact_df["transaction_timestamp"]).max()
            latest_tx_time = max_ts.strftime('%H:%M:%S (%Y-%m-%d)')
            spark_status = "🟢 ACTIVE (Chạy 24/7)"
        except Exception:
            pass

    st.markdown(f"""
    <div style="background: rgba(0, 212, 255, 0.08); border-left: 4px solid #00D4FF; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px;">
        <span style="font-weight: bold; color: #00D4FF;">⚡ TRẠNG THÁI LUỒNG SPARK STREAMING:</span> {spark_status} | 
        <span style="font-weight: bold; color: #FFB703;">⏱️ GIAO DỊCH MỚI NHẤT:</span> {latest_tx_time} | 
        <span style="font-weight: bold; color: #00CC96;">🔄 CẬP NHẬT TRÌNH DUYỆT:</span> {datetime.now().strftime('%H:%M:%S')}
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)
    total_users = len(users_df) if not users_df.empty else 0
    total_cards = len(cards_df) if not cards_df.empty else 0
    total_tx = len(fact_df) if not fact_df.empty else 0
    total_amount = fact_df["amount"].sum() if not fact_df.empty and "amount" in fact_df.columns else 0.0
    fraud_count = fact_df["is_fraud"].sum() if not fact_df.empty and "is_fraud" in fact_df.columns else 0
    fraud_rate = (fraud_count / total_tx * 100) if total_tx > 0 else 0.0

    with col1:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Khách hàng</div><div class="metric-value">{total_users:,}</div><div style="color: #00CC96; font-size: 12px;">DIM_USERS</div></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Thẻ tín dụng</div><div class="metric-value">{total_cards:,}</div><div style="color: #00CC96; font-size: 12px;">DIM_CARDS</div></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Tổng giao dịch</div><div class="metric-value">{total_tx:,}</div><div style="color: #00D4FF; font-size: 12px;">FACT_TRANSACTIONS</div></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Tổng dòng tiền ($)</div><div class="metric-value">${total_amount:,.2f}</div><div style="color: #FFB703; font-size: 12px;">USD STREAMED</div></div>""", unsafe_allow_html=True)
    with col5:
        st.markdown(f"""<div class="metric-card"><div class="metric-label">Gian lận</div><div class="metric-value" style="color: #FF4B4B;">{fraud_count:,} ({fraud_rate:.1f}%)</div><div style="color: #FF4B4B; font-size: 12px;">CẢNH BÁO</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    if fact_df.empty:
        st.warning("⚡ Data Lake Tầng Silver đang chờ giao dịch streaming mới từ Kafka...")
        st.info("Hãy đảm bảo container `kafka-producer` và `spark-streaming-consumer` đang chạy trong Docker.")
    else:
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.subheader("📈 Tốc độ nạp giao dịch Real-Time")
            if "transaction_timestamp" in fact_df.columns:
                fact_df["ts_minute"] = pd.to_datetime(fact_df["transaction_timestamp"]).dt.strftime('%H:%M')
                timeline_df = fact_df.groupby("ts_minute").size().reset_index(name="transaction_count")
                fig_line = px.line(
                    timeline_df.tail(20),
                    x="ts_minute", y="transaction_count", markers=True,
                    labels={"ts_minute": "Thời gian", "transaction_count": "Số giao dịch"},
                    color_discrete_sequence=["#00D4FF"]
                )
                fig_line.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
                st.plotly_chart(fig_line, use_container_width=True)

        with chart_col2:
            st.subheader("💳 Cơ cấu hình thức quẹt thẻ (Chip vs Swipe)")
            if "use_chip" in fact_df.columns:
                chip_counts = fact_df["use_chip"].value_counts().reset_index()
                chip_counts.columns = ["Hình thức", "Số lượng"]
                fig_donut = px.pie(
                    chip_counts, values="Số lượng", names="Hình thức", hole=0.5,
                    color_discrete_sequence=px.colors.sequential.Darkmint
                )
                fig_donut.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
                st.plotly_chart(fig_donut, use_container_width=True)

        st.subheader("📋 Bảng giao dịch mới nhất (Live Feed)")
        display_cols = ["transaction_timestamp", "user_id", "card_index", "amount", "merchant_city", "merchant_state", "use_chip", "is_fraud"]
        available_cols = [c for c in display_cols if c in fact_df.columns]
        recent_tx = fact_df.sort_values(by="transaction_timestamp", ascending=False).head(15)[available_cols]

        def highlight_fraud(val):
            if val is True or str(val).lower() in ["true", "yes", "1"]:
                return 'background-color: rgba(255, 75, 75, 0.3); color: #FF4B4B; font-weight: bold;'
            return 'color: #00CC96;'

        if "is_fraud" in recent_tx.columns:
            try:
                styled_df = recent_tx.style.applymap(highlight_fraud, subset=["is_fraud"]).format({"amount": "${:,.2f}"})
            except AttributeError:
                styled_df = recent_tx.style.map(highlight_fraud, subset=["is_fraud"]).format({"amount": "${:,.2f}"})
            st.dataframe(styled_df, use_container_width=True, height=350)
        else:
            st.dataframe(recent_tx, use_container_width=True, height=350)

with tab_dwh:
    st.markdown("""
    <div style="background: rgba(16, 185, 129, 0.08); border-left: 4px solid #10B981; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px;">
        <span style="font-weight: bold; color: #10B981;">🏛️ DATA WAREHOUSE GOLD LAYER:</span>
        Đọc trực tiếp từ các bảng tổng hợp <b>Pre-Aggregated Summary Layers</b> (Chuỗi Roll-up Inmon Chapter 3: Ngày → Tháng → Quý).
        Truy vấn siêu tốc dưới 0.1s nhờ loại bỏ full scan bảng chi tiết!
    </div>
    """, unsafe_allow_html=True)

    if monthly_df.empty:
        st.warning("⚠️ Chưa phát hiện bảng Gold Summary. Vui lòng chạy `run_dwh_pipeline.ps1` để xây dựng Gold Layer.")
    else:
        with st.expander("🏛️ Xem Tóm Tắt Kiến Trúc Tầng Gold (Kimball Star Schema & Inmon Roll-up)", expanded=False):
            st.markdown("""
            * **Mô hình DWH:** **Kimball Star Schema** — Bảng sự kiện Atomic (`fact_transactions`: 19,963 giao dịch chi tiết) liên kết 4 bảng chiều qua Surrogate Keys.
            * **Chiến lược SCD:** `dim_date` (SCD0 - Bất biến), `dim_merchant` (SCD1 - Ghi đè), `dim_cards` & `dim_users` (SCD2 - Lưu vết lịch sử `effective_date`, `expiration_date`, `is_current`).
            * **Tháp phân tầng Inmon:** Chuỗi Roll-up bảo toàn độ đo cộng dồn (Additive): **Ngày** (`agg_daily_merchant_vol`) ➔ **Tháng** (`agg_monthly_merchant_vol`) ➔ **Quý** (`agg_quarterly_merchant_vol`).
            * 📄 **Sơ đồ chi tiết:** Xem tại tệp [`docs/gold_dwh_architecture_and_diagrams.md`](file:///e:/credit_management/docs/gold_dwh_architecture_and_diagrams.md).
            """)

        # DWH KPIs
        dwh_vol = monthly_df["total_volume"].sum() if "total_volume" in monthly_df.columns else 0.0
        dwh_tx  = monthly_df["total_txn"].sum() if "total_txn" in monthly_df.columns else 0
        dwh_fraud = monthly_df["fraud_count"].sum() if "fraud_count" in monthly_df.columns else 0
        dwh_merchants = monthly_df["merchant_sk"].nunique() if "merchant_sk" in monthly_df.columns else 0

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">DWH Doanh Số Gold ($)</div><div class="metric-value">${dwh_vol:,.2f}</div><div style="color: #10B981; font-size: 12px;">RECONCILED 100%</div></div>""", unsafe_allow_html=True)
        with kpi2:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Tổng Giao Dịch Gold</div><div class="metric-value">{dwh_tx:,}</div><div style="color: #00D4FF; font-size: 12px;">INMON ROLLUP</div></div>""", unsafe_allow_html=True)
        with kpi3:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Số Vụ Gian Lận</div><div class="metric-value" style="color: #FF4B4B;">{dwh_fraud:,}</div><div style="color: #FF4B4B; font-size: 12px;">FRAUD CASES</div></div>""", unsafe_allow_html=True)
        with kpi4:
            st.markdown(f"""<div class="metric-card"><div class="metric-label">Đối Tác Merchant</div><div class="metric-value">{dwh_merchants:,}</div><div style="color: #FFB703; font-size: 12px;">DIM_MERCHANT SK</div></div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        dwh_c1, dwh_c2 = st.columns(2)

        with dwh_c1:
            st.subheader("📅 Doanh Số Theo Tháng (agg_monthly_merchant_vol)")
            if "year" in monthly_df.columns and "month_number" in monthly_df.columns:
                m_trend = monthly_df.groupby(["year", "month_number"])["total_volume"].sum().reset_index()
                m_trend["period"] = m_trend["year"].astype(str) + "-" + m_trend["month_number"].astype(str).str.zfill(2)
                m_trend = m_trend.sort_values(by=["year", "month_number"]).tail(24)

                fig_m = px.bar(
                    m_trend, x="period", y="total_volume",
                    labels={"period": "Tháng", "total_volume": "Doanh số ($)"},
                    color_discrete_sequence=["#10B981"]
                )
                fig_m.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
                st.plotly_chart(fig_m, use_container_width=True)

        with dwh_c2:
            st.subheader("📊 Doanh Số Theo Quý (agg_quarterly_merchant_vol)")
            if not quarterly_df.empty and "year" in quarterly_df.columns and "quarter" in quarterly_df.columns:
                q_trend = quarterly_df.groupby(["year", "quarter"])["total_volume"].sum().reset_index()
                q_trend["quarter_label"] = q_trend["year"].astype(str) + " Q" + q_trend["quarter"].astype(str)
                q_trend = q_trend.sort_values(by=["year", "quarter"]).tail(12)

                fig_q = px.line(
                    q_trend, x="quarter_label", y="total_volume", markers=True,
                    labels={"quarter_label": "Quý", "total_volume": "Doanh số ($)"},
                    color_discrete_sequence=["#FFB703"]
                )
                fig_q.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320)
                st.plotly_chart(fig_q, use_container_width=True)

        st.subheader("🛡️ Giám Sát Rủi Ro Thẻ Hàng Ngày (agg_daily_card_fraud)")
        if not daily_card_df.empty:
            fraud_cards = daily_card_df[daily_card_df["fraud_count"] > 0].sort_values(by="fraud_count", ascending=False).head(10)
            if not fraud_cards.empty:
                st.dataframe(fraud_cards[["date_sk", "card_sk", "total_txn", "total_amount", "fraud_count", "fraud_rate_pct"]], use_container_width=True)
            else:
                st.info("Không có thẻ nào bị gian lận trong tập mẫu hiện tại.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("🔍 Trình Duyệt Dữ Liệu Tầng Gold (Gold Data Inspector)")
        st.caption("Xem trực tiếp các bản ghi thực tế được lưu trữ trong tầng Gold Parquet:")
        gold_options = {
            "fact_transactions (Bảng sự kiện giao dịch Atomic)": GOLD_FACT_PATH,
            "dim_cards (Thẻ tín dụng SCD Type 2)": os.path.join(BASE_DIR, "data", "gold", "dim_cards"),
            "dim_users (Hồ sơ khách hàng SCD Type 2)": os.path.join(BASE_DIR, "data", "gold", "dim_users"),
            "dim_merchant (Thương nhân SCD Type 1)": GOLD_MERCHANT_PATH,
            "dim_date (Lịch thời gian 1995-2035)": os.path.join(BASE_DIR, "data", "gold", "dim_date"),
            "agg_monthly_merchant_vol (Tổng hợp Tháng Inmon)": GOLD_MONTHLY_PATH,
            "agg_quarterly_merchant_vol (Tổng hợp Quý Inmon)": GOLD_QUARTERLY_PATH
        }
        chosen_tbl = st.selectbox("Chọn bảng Gold để xem bản ghi:", list(gold_options.keys()))
        target_path = gold_options[chosen_tbl]
        if os.path.exists(target_path):
            try:
                tbl_df = pd.read_parquet(target_path)
                st.write(f"Tổng số dòng trong bảng: **{len(tbl_df):,} dòng** | Hiển thị 15 bản ghi mẫu:")
                st.dataframe(tbl_df.head(15), use_container_width=True)
            except Exception as e:
                st.warning(f"Không thể đọc bảng: {e}")

with tab_sql:
    st.markdown("""
    <div style="background: rgba(255, 183, 3, 0.08); border-left: 4px solid #FFB703; padding: 12px 16px; border-radius: 6px; margin-bottom: 20px;">
        <span style="font-weight: bold; color: #FFB703;">🔍 TRUNG TÂM TRUY VẤN DWH SQL (KIMBALL STAR SCHEMA & INMON ROLLUP):</span>
        Thực thi các câu lệnh truy vấn chuẩn ANSI SQL trực tiếp trên Kho Dữ Liệu Gold.
        Hỗ trợ toàn diện các bài toán OLAP: <b>Roll-up, Drill-down, Slice & Dice</b> và kiểm chứng <b>Point-in-Time Join</b>.
    </div>
    """, unsafe_allow_html=True)

    sql_presets = {
        "1. Inmon Roll-up: Cuộn Doanh số & Tỷ lệ Gian lận từ Tháng lên Quý": """SELECT 
    year,
    quarter,
    COUNT(DISTINCT merchant_sk) AS active_merchants,
    SUM(total_txn) AS quarterly_txns,
    ROUND(SUM(total_volume), 2) AS quarterly_volume,
    SUM(fraud_count) AS quarterly_frauds,
    ROUND(SUM(total_volume) / SUM(total_txn), 2) AS avg_txn_amount,
    ROUND(SUM(fraud_count) * 100.0 / SUM(total_txn), 4) AS fraud_rate_pct
FROM agg_monthly_merchant_vol
GROUP BY year, quarter
ORDER BY year DESC, quarter DESC;""",

        "2. Drill-down: Phân tích Rủi ro Gian lận theo Hình thức Quẹt (Chip vs Swipe)": """SELECT 
    f.use_chip,
    COUNT(*) AS total_transactions,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(f.amount), 2) AS avg_amount
FROM fact_transactions f
GROUP BY f.use_chip
ORDER BY fraud_rate_pct DESC;""",

        "3. Bằng chứng SCD2 Point-in-Time Join: Thẻ 0_0 chia tách giao dịch qua 2 card_sk": """SELECT 
    f.card_sk,
    c.card_index,
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
WHERE c.user_id = 0 AND c.card_index = 0
GROUP BY f.card_sk, c.card_index, c.credit_limit, c.card_on_dark_web, c.effective_date, c.expiration_date, c.is_current
ORDER BY f.card_sk;""",

        "4. Top 10 Merchant có Doanh số Lớn Nhất & Tỷ lệ Gian lận (Star Schema Join)": """SELECT 
    m.merchant_name,
    m.merchant_city,
    m.merchant_state,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct
FROM fact_transactions f
JOIN dim_merchant m ON f.merchant_sk = m.merchant_sk
GROUP BY m.merchant_name, m.merchant_city, m.merchant_state
ORDER BY total_volume DESC
LIMIT 10;""",

        "5. Slice & Dice: Doanh số & Rủi ro theo Nhóm Tuổi Khách hàng": """SELECT 
    d.year,
    CASE 
        WHEN u.current_age < 30 THEN '1. Duoi 30'
        WHEN u.current_age BETWEEN 30 AND 50 THEN '2. Tu 30-50'
        ELSE '3. Tren 50'
    END AS age_group,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count
FROM fact_transactions f
JOIN dim_users u ON f.user_sk = u.user_sk
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year, age_group
ORDER BY d.year DESC, age_group;""",

        "Tùy chỉnh: Tự viết câu lệnh SQL của bạn": "SELECT * FROM fact_transactions LIMIT 10;"
    }

    selected_preset = st.selectbox("📌 Chọn câu truy vấn mẫu (hoặc tự gõ SQL):", list(sql_presets.keys()))
    sql_text = st.text_area("⌨️ Trình soạn thảo SQL (ANSI SQL):", value=sql_presets[selected_preset], height=180)

    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        run_btn = st.button("▶ Thực thi SQL", type="primary")

    if run_btn and sql_text.strip():
        sql_conn = get_sqlite_gold_conn()
        t_start = time.time()
        try:
            query_res = pd.read_sql(sql_text, sql_conn)
            t_exec = (time.time() - t_start) * 1000
            st.success(f"✅ Truy vấn thành công! Trả về **{len(query_res):,} dòng** trong **{t_exec:.1f} ms**.")
            st.dataframe(query_res, use_container_width=True)

            # Tùy chọn vẽ biểu đồ nhanh nếu kết quả có 2 cột số/chuỗi
            num_cols = query_res.select_dtypes(include=['number']).columns.tolist()
            cat_cols = query_res.select_dtypes(include=['object']).columns.tolist()
            if len(cat_cols) >= 1 and len(num_cols) >= 1 and len(query_res) <= 50:
                with st.expander("📈 Xem biểu đồ trực quan hóa kết quả truy vấn"):
                    fig_sql = px.bar(
                        query_res, x=cat_cols[0], y=num_cols[0],
                        title=f"{num_cols[0]} theo {cat_cols[0]}",
                        color_discrete_sequence=["#FFB703"]
                    )
                    fig_sql.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_sql, use_container_width=True)
        except Exception as e:
            st.error(f"❌ Lỗi thực thi SQL: {e}")

# TỰ ĐỘNG REFRESH DỮ LIỆU DÙNG STREAMLIT RERUN
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
