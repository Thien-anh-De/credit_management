import os
import time
import sqlite3
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# 1. CẤU HÌNH TRANG WEB CHUẨN DOANH NGHIỆP
st.set_page_config(
    page_title="Credit Management & DWH Platform",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. CSS GIAO DIỆN HIỆN ĐẠI (DARK FINTECH THEME - KHÔNG CHỚP NHÁY)
st.markdown("""
<style>
    /* Nền chính */
    .stApp {
        background-color: #0B0E14;
        color: #E2E8F0;
    }
    
    /* Ẩn bớt padding thừa */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Thẻ chỉ số KPI cao cấp */
    .metric-card {
        background: #151A23;
        border: 1px solid #232B3B;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 15px;
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
    }
    .metric-title {
        font-size: 13px;
        font-weight: 600;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    .metric-number {
        font-size: 26px;
        font-weight: 700;
        color: #F8FAFC;
        margin-top: 4px;
    }
    .metric-badge {
        display: inline-block;
        font-size: 11px;
        font-weight: 600;
        padding: 2px 8px;
        border-radius: 4px;
        margin-top: 6px;
    }
    .badge-cyan { background: rgba(6, 182, 212, 0.15); color: #22D3EE; }
    .badge-green { background: rgba(16, 185, 129, 0.15); color: #34D399; }
    .badge-amber { background: rgba(245, 158, 11, 0.15); color: #FBBF24; }
    .badge-red { background: rgba(239, 68, 68, 0.15); color: #F87171; }
    
    /* Thanh trạng thái Status Bar */
    .status-bar {
        background: #151A23;
        border-left: 4px solid #06B6D4;
        border-radius: 6px;
        padding: 12px 18px;
        margin-bottom: 20px;
        font-size: 14px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
</style>
""", unsafe_allow_html=True)

# 3. ĐƯỜNG DẪN DỮ LIỆU
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

SILVER_USERS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_users")
SILVER_CARDS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_cards")
SILVER_FACT_PATH  = os.path.join(BASE_DIR, "data", "silver", "fact_transactions")

GOLD_FACT_PATH       = os.path.join(BASE_DIR, "data", "gold", "fact_transactions")
GOLD_DAILY_MERCH_PATH = os.path.join(BASE_DIR, "data", "gold", "agg_daily_merchant_vol")
GOLD_MONTHLY_PATH    = os.path.join(BASE_DIR, "data", "gold", "agg_monthly_merchant_vol")
GOLD_QUARTERLY_PATH  = os.path.join(BASE_DIR, "data", "gold", "agg_quarterly_merchant_vol")
GOLD_DAILY_CARD_PATH = os.path.join(BASE_DIR, "data", "gold", "agg_daily_card_fraud")
GOLD_MERCHANT_PATH   = os.path.join(BASE_DIR, "data", "gold", "dim_merchant")


# 4. HÀM TẢI DỮ LIỆU TỐI ƯU
@st.cache_data(ttl=3)
def load_silver_data():
    users_df, cards_df, fact_df = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    if os.path.exists(SILVER_USERS_PATH):
        try: users_df = pd.read_parquet(SILVER_USERS_PATH)
        except Exception: pass
    if os.path.exists(SILVER_CARDS_PATH):
        try: cards_df = pd.read_parquet(SILVER_CARDS_PATH)
        except Exception: pass
    if os.path.exists(SILVER_FACT_PATH):
        try: fact_df = pd.read_parquet(SILVER_FACT_PATH)
        except Exception: pass
    return users_df, cards_df, fact_df


@st.cache_data(ttl=10)
def load_gold_data():
    monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df = (
        pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    )
    if os.path.exists(GOLD_MONTHLY_PATH):
        try: monthly_df = pd.read_parquet(GOLD_MONTHLY_PATH)
        except Exception: pass
    if os.path.exists(GOLD_QUARTERLY_PATH):
        try: quarterly_df = pd.read_parquet(GOLD_QUARTERLY_PATH)
        except Exception: pass
    if os.path.exists(GOLD_DAILY_CARD_PATH):
        try: daily_card_df = pd.read_parquet(GOLD_DAILY_CARD_PATH)
        except Exception: pass
    if os.path.exists(GOLD_DAILY_MERCH_PATH):
        try: daily_merch_df = pd.read_parquet(GOLD_DAILY_MERCH_PATH)
        except Exception: pass
    if os.path.exists(GOLD_MERCHANT_PATH):
        try: merchant_df = pd.read_parquet(GOLD_MERCHANT_PATH)
        except Exception: pass
    return monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df


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


# 5. SIDEBAR ĐIỀU HƯỚNG BỀN VỮNG (KHÔNG DÙNG ST.TABS ĐỂ TRÁNH RESET TRANG)
st.sidebar.markdown("## 💳 Credit Platform")
selected_page = st.sidebar.radio(
    "CHỨC NĂNG HỆ THỐNG",
    [
        "⚡ Giám Sát Streaming Real-time",
        "🏛️ Báo Cáo Tổng Hợp DWH",
        "🔍 Truy Vấn SQL Trực Tiếp",
        "📁 Duyệt Bảng Dữ Liệu Chi Tiết"
    ],
    index=0,
    key="main_navigation"
)

st.sidebar.markdown("---")

# Quản lý luồng làm mới chỉ áp dụng cho trang Streaming
if selected_page == "⚡ Giám Sát Streaming Real-time":
    auto_refresh = st.sidebar.checkbox("Tự động cập nhật giao dịch (3s)", value=False)
    if auto_refresh:
        st.sidebar.info("Đang bật chế độ Live Feed.")
else:
    auto_refresh = False

if st.sidebar.button("🔄 Làm mới dữ liệu", use_container_width=True):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("**Hạ tầng phân tán:**")
st.sidebar.markdown("- ⚡ [Spark Master UI](http://localhost:8080)")
st.sidebar.markdown("- 👷 [Spark Worker UI](http://localhost:8081)")


# ════════════════════════════════════════════════════════════════════════════
# TRANG 1: GIÁM SÁT STREAMING REAL-TIME
# ════════════════════════════════════════════════════════════════════════════
if selected_page == "⚡ Giám Sát Streaming Real-time":
    st.title("⚡ Giám Sát Giao Dịch Thời Gian Thực (Streaming POS)")
    
    users_df, cards_df, fact_df = load_silver_data()
    
    # Tính toán trạng thái
    latest_tx_time = "Chưa có dữ liệu"
    spark_status = "INACTIVE"
    badge_status = "badge-red"
    if not fact_df.empty and "transaction_timestamp" in fact_df.columns:
        try:
            max_ts = pd.to_datetime(fact_df["transaction_timestamp"]).max()
            latest_tx_time = max_ts.strftime('%H:%M:%S (%Y-%m-%d)')
            spark_status = "ACTIVE (24/7)"
            badge_status = "badge-green"
        except Exception:
            pass

    st.markdown(f"""
    <div class="status-bar">
        <div><b>LUỒNG SPARK STREAMING:</b> <span class="metric-badge {badge_status}">{spark_status}</span></div>
        <div><b>GIAO DỊCH MỚI NHẤT:</b> <span style="color: #FBBF24; font-weight: 600;">{latest_tx_time}</span></div>
        <div><b>ĐỒNG HỒ TRÌNH DUYỆT:</b> <span style="color: #94A3B8;">{datetime.now().strftime('%H:%M:%S')}</span></div>
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
        st.markdown(f"""<div class="metric-card"><div class="metric-title">Khách hàng</div><div class="metric-number">{total_users:,}</div><span class="metric-badge badge-green">Tầng Silver</span></div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="metric-card"><div class="metric-title">Thẻ tín dụng</div><div class="metric-number">{total_cards:,}</div><span class="metric-badge badge-green">Tầng Silver</span></div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="metric-card"><div class="metric-title">Tổng giao dịch</div><div class="metric-number">{total_tx:,}</div><span class="metric-badge badge-cyan">Lưu Parquet</span></div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="metric-card"><div class="metric-title">Tổng dòng tiền</div><div class="metric-number">${total_amount:,.2f}</div><span class="metric-badge badge-amber">USD Streamed</span></div>""", unsafe_allow_html=True)
    with col5:
        st.markdown(f"""<div class="metric-card"><div class="metric-title">Giao dịch gian lận</div><div class="metric-number" style="color: #F87171;">{fraud_count:,} ({fraud_rate:.1f}%)</div><span class="metric-badge badge-red">Cảnh báo</span></div>""", unsafe_allow_html=True)

    if fact_df.empty:
        st.warning("Data Lake Tầng Silver đang chờ giao dịch streaming mới từ Kafka...")
    else:
        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.subheader("Tốc độ nạp giao dịch theo phút")
            if "transaction_timestamp" in fact_df.columns:
                fact_df["ts_minute"] = pd.to_datetime(fact_df["transaction_timestamp"]).dt.strftime('%H:%M')
                timeline_df = fact_df.groupby("ts_minute").size().reset_index(name="transaction_count")
                fig_line = px.line(
                    timeline_df.tail(20),
                    x="ts_minute", y="transaction_count", markers=True,
                    labels={"ts_minute": "Thời gian", "transaction_count": "Số giao dịch"},
                    color_discrete_sequence=["#06B6D4"]
                )
                fig_line.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_line, use_container_width=True)

        with chart_col2:
            st.subheader("Cơ cấu hình thức quẹt thẻ")
            if "use_chip" in fact_df.columns:
                chip_counts = fact_df["use_chip"].value_counts().reset_index()
                chip_counts.columns = ["Hình thức", "Số lượng"]
                fig_donut = px.pie(
                    chip_counts, values="Số lượng", names="Hình thức", hole=0.55,
                    color_discrete_sequence=px.colors.sequential.Teal
                )
                fig_donut.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=300, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_donut, use_container_width=True)

        st.subheader("Giao dịch mới nhất (Live Feed)")
        display_cols = ["transaction_timestamp", "user_id", "card_index", "amount", "merchant_city", "merchant_state", "use_chip", "is_fraud"]
        available_cols = [c for c in display_cols if c in fact_df.columns]
        recent_tx = fact_df.sort_values(by="transaction_timestamp", ascending=False).head(15)[available_cols]

        def highlight_fraud(val):
            if val is True or str(val).lower() in ["true", "yes", "1"]:
                return 'background-color: rgba(239, 68, 68, 0.25); color: #F87171; font-weight: bold;'
            return 'color: #34D399;'

        try:
            styled_df = recent_tx.style.map(highlight_fraud, subset=["is_fraud"]).format({"amount": "${:,.2f}"})
        except Exception:
            styled_df = recent_tx
        st.dataframe(styled_df, use_container_width=True, height=350)

    # Tự động refresh an toàn (chỉ ở trang Streaming)
    if auto_refresh:
        time.sleep(3)
        st.rerun()


# ════════════════════════════════════════════════════════════════════════════
# TRANG 2: BÁO CÁO TỔNG HỢP DWH (GOLD ANALYTICS)
# ════════════════════════════════════════════════════════════════════════════
elif selected_page == "🏛️ Báo Cáo Tổng Hợp DWH":
    st.title("🏛️ Báo Cáo Tổng Hợp Doanh Số & Rủi Ro (Tầng Gold)")
    
    monthly_df, quarterly_df, daily_card_df, daily_merch_df, merchant_df = load_gold_data()

    if monthly_df.empty:
        st.warning("Chưa có dữ liệu tầng Gold. Vui lòng chạy lệnh: .\\run_dwh_pipeline.ps1")
    else:
        dwh_vol = monthly_df["total_volume"].sum() if "total_volume" in monthly_df.columns else 0.0
        dwh_tx  = monthly_df["total_txn"].sum() if "total_txn" in monthly_df.columns else 0
        dwh_fraud = monthly_df["fraud_count"].sum() if "fraud_count" in monthly_df.columns else 0
        dwh_merchants = monthly_df["merchant_sk"].nunique() if "merchant_sk" in monthly_df.columns else 0

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown(f"""<div class="metric-card"><div class="metric-title">Doanh số toàn hệ thống</div><div class="metric-number">${dwh_vol:,.2f}</div><span class="metric-badge badge-green">Đối soát 100%</span></div>""", unsafe_allow_html=True)
        with k2:
            st.markdown(f"""<div class="metric-card"><div class="metric-title">Tổng số giao dịch</div><div class="metric-number">{dwh_tx:,}</div><span class="metric-badge badge-cyan">Gold Star Schema</span></div>""", unsafe_allow_html=True)
        with k3:
            st.markdown(f"""<div class="metric-card"><div class="metric-title">Số vụ gian lận</div><div class="metric-number" style="color: #F87171;">{dwh_fraud:,}</div><span class="metric-badge badge-red">Đã kiểm soát</span></div>""", unsafe_allow_html=True)
        with k4:
            st.markdown(f"""<div class="metric-card"><div class="metric-title">Tổng số điểm bán (POS)</div><div class="metric-number">{dwh_merchants:,}</div><span class="metric-badge badge-amber">Thương nhân</span></div>""", unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Doanh số theo tháng")
            if "year" in monthly_df.columns and "month_number" in monthly_df.columns:
                m_trend = monthly_df.groupby(["year", "month_number"])["total_volume"].sum().reset_index()
                m_trend["period"] = m_trend["year"].astype(str) + "-" + m_trend["month_number"].astype(str).str.zfill(2)
                m_trend = m_trend.sort_values(by=["year", "month_number"]).tail(18)

                fig_m = px.bar(
                    m_trend, x="period", y="total_volume",
                    labels={"period": "Tháng", "total_volume": "Doanh số ($)"},
                    color_discrete_sequence=["#10B981"]
                )
                fig_m.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_m, use_container_width=True)

        with c2:
            st.subheader("Doanh số theo quý")
            if not quarterly_df.empty and "year" in quarterly_df.columns and "quarter" in quarterly_df.columns:
                q_trend = quarterly_df.groupby(["year", "quarter"])["total_volume"].sum().reset_index()
                q_trend["quarter_label"] = q_trend["year"].astype(str) + " Q" + q_trend["quarter"].astype(str)
                q_trend = q_trend.sort_values(by=["year", "quarter"]).tail(12)

                fig_q = px.line(
                    q_trend, x="quarter_label", y="total_volume", markers=True,
                    labels={"quarter_label": "Quý", "total_volume": "Doanh số ($)"},
                    color_discrete_sequence=["#F59E0B"]
                )
                fig_q.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=320, margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig_q, use_container_width=True)

        st.subheader("Top thẻ có dấu hiệu gian lận cao nhất")
        if not daily_card_df.empty:
            fraud_cards = daily_card_df[daily_card_df["fraud_count"] > 0].sort_values(by="fraud_count", ascending=False).head(10)
            if not fraud_cards.empty:
                st.dataframe(fraud_cards[["date_sk", "card_sk", "total_txn", "total_amount", "fraud_count", "fraud_rate_pct"]], use_container_width=True)
            else:
                st.info("Không phát hiện thẻ có rủi ro gian lận trong tập mẫu.")


# ════════════════════════════════════════════════════════════════════════════
# TRANG 3: TRUY VẤN SQL TRỰC TIẾP (SQL STUDIO)
# ════════════════════════════════════════════════════════════════════════════
elif selected_page == "🔍 Truy Vấn SQL Trực Tiếp":
    st.title("🔍 Trung Tâm Truy Vấn SQL (Star Schema)")
    
    sql_presets = {
        "1. Doanh số & Tỷ lệ gian lận theo Quý": """SELECT 
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

        "2. Phân tích rủi ro theo hình thức quẹt thẻ": """SELECT 
    f.use_chip,
    COUNT(*) AS total_transactions,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count,
    ROUND(SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) AS fraud_rate_pct,
    ROUND(AVG(f.amount), 2) AS avg_amount
FROM fact_transactions f
GROUP BY f.use_chip
ORDER BY fraud_rate_pct DESC;""",

        "3. Kiểm chứng lịch sử thẻ (SCD Type 2 Point-in-Time)": """SELECT 
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

        "4. Top 10 đối tác điểm bán có doanh thu cao nhất": """SELECT 
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

        "5. Phân nhóm chi tiêu theo độ tuổi khách hàng": """SELECT 
    d.year,
    CASE 
        WHEN u.current_age < 30 THEN 'Dưới 30 tuổi'
        WHEN u.current_age BETWEEN 30 AND 50 THEN '30 - 50 tuổi'
        ELSE 'Trên 50 tuổi'
    END AS age_group,
    COUNT(*) AS total_txns,
    ROUND(SUM(f.amount), 2) AS total_volume,
    SUM(CASE WHEN f.is_fraud THEN 1 ELSE 0 END) AS fraud_count
FROM fact_transactions f
JOIN dim_users u ON f.user_sk = u.user_sk
JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY d.year, age_group
ORDER BY d.year DESC, age_group;""",

        "Tùy chỉnh: Tự viết câu lệnh SQL": "SELECT * FROM fact_transactions LIMIT 10;"
    }

    selected_preset = st.selectbox("Chọn câu truy vấn nghiệp vụ có sẵn:", list(sql_presets.keys()))
    sql_text = st.text_area("Câu lệnh SQL (Chuẩn ANSI SQL):", value=sql_presets[selected_preset], height=160)

    if st.button("▶ Chạy truy vấn", type="primary"):
        if sql_text.strip():
            sql_conn = get_sqlite_gold_conn()
            t_start = time.time()
            try:
                query_res = pd.read_sql(sql_text, sql_conn)
                t_exec = (time.time() - t_start) * 1000
                st.success(f"Truy vấn thành công: **{len(query_res):,} dòng** trong **{t_exec:.1f} ms**")
                st.dataframe(query_res, use_container_width=True)
            except Exception as e:
                st.error(f"Lỗi cú pháp SQL: {e}")


# ════════════════════════════════════════════════════════════════════════════
# TRANG 4: DUYỆT BẢNG DỮ LIỆU CHI TIẾT (DATA INSPECTOR)
# ════════════════════════════════════════════════════════════════════════════
elif selected_page == "📁 Duyệt Bảng Dữ Liệu Chi Tiết":
    st.title("📁 Duyệt Dữ Liệu Tầng Gold (Data Inspector)")
    
    gold_options = {
        "fact_transactions (Bảng sự kiện giao dịch chi tiết)": GOLD_FACT_PATH,
        "dim_cards (Thẻ tín dụng - SCD Type 2)": os.path.join(BASE_DIR, "data", "gold", "dim_cards"),
        "dim_users (Khách hàng - SCD Type 2)": os.path.join(BASE_DIR, "data", "gold", "dim_users"),
        "dim_merchant (Thương nhân POS - SCD Type 1)": GOLD_MERCHANT_PATH,
        "dim_date (Lịch thời gian 1995-2035)": os.path.join(BASE_DIR, "data", "gold", "dim_date"),
        "agg_monthly_merchant_vol (Tổng hợp Tháng)": GOLD_MONTHLY_PATH,
        "agg_quarterly_merchant_vol (Tổng hợp Quý)": GOLD_QUARTERLY_PATH,
        "agg_daily_card_fraud (Rủi ro thẻ theo ngày)": GOLD_DAILY_CARD_PATH
    }

    chosen_tbl = st.selectbox("Chọn bảng cần kiểm tra:", list(gold_options.keys()))
    target_path = gold_options[chosen_tbl]

    if os.path.exists(target_path):
        try:
            tbl_df = pd.read_parquet(target_path)
            st.write(f"Tổng số bản ghi: **{len(tbl_df):,} dòng** | Hiển thị 20 dòng đầu tiên:")
            st.dataframe(tbl_df.head(20), use_container_width=True)
        except Exception as e:
            st.error(f"Không thể đọc file Parquet: {e}")
    else:
        st.warning(f"Chưa tìm thấy dữ liệu tại: {target_path}")
