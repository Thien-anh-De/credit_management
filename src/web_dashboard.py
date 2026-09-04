import os
import glob
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# CẤU HÌNH TRANG WEB
st.set_page_config(
    page_title="management dashboard",
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
        font-size: 32px;
        font-weight: 700;
        color: #00D4FF;
    }
    .metric-label {
        font-size: 14px;
        color: #A0AAB0;
        text-transform: uppercase;
        letter-spacing: 1px;
    }
</style>
""", unsafe_allow_html=True)

# XÁC ĐỊNH ĐƯỜNG DẪN TỚI THƯ MỤC DATA LAKE (SILVER ZONE)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SILVER_USERS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_users")
SILVER_CARDS_PATH = os.path.join(BASE_DIR, "data", "silver", "dim_cards")
SILVER_FACT_PATH = os.path.join(BASE_DIR, "data", "silver", "fact_transactions")

#HÀM ĐỌC DỮ LIỆU TỪ CÁC FILE PARQUET (CACHE 2 GIÂY)
@st.cache_data(ttl=2)
def load_silver_data():
    users_df = pd.DataFrame()
    cards_df = pd.DataFrame()
    fact_df = pd.DataFrame()

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

#TẠO THANH ĐIỀU HƯỚNG CÀI ĐẶT (SIDEBAR)
st.sidebar.title("⚙️ Cài đặt Dashboard")
auto_refresh = st.sidebar.checkbox("Tự động làm mới (3s)", value=True)
refresh_interval = st.sidebar.slider("Tần suất làm mới (giây)", 1, 10, 3)

if st.sidebar.button("🔄 Làm mới thủ công"):
    st.cache_data.clear()


st.sidebar.markdown("### 🖥️ Công cụ quản lý Cluster")
st.sidebar.markdown("- ⚡ [Spark Master UI](http://localhost:8080)")
st.sidebar.markdown("- 👷 [Spark Worker UI](http://localhost:8081)")

# TIÊU ĐỀ VÀ THẺ THỐNG KÊ KPI NỔI BẬT
st.title("💳 Management Dashboard")

users_df, cards_df, fact_df = load_silver_data()

# Tính toán trạng thái luồng Spark
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
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Khách hàng</div>
        <div class="metric-value">{total_users:,}</div>
        <div style="color: #00CC96; font-size: 12px;">DIM_USERS</div>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Thẻ tín dụng</div>
        <div class="metric-value">{total_cards:,}</div>
        <div style="color: #00CC96; font-size: 12px;">DIM_CARDS</div>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Tổng giao dịch</div>
        <div class="metric-value">{total_tx:,}</div>
        <div style="color: #00D4FF; font-size: 12px;">FACT_TRANSACTIONS</div>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Tổng dòng tiền ($)</div>
        <div class="metric-value">${total_amount:,.2f}</div>
        <div style="color: #FFB703; font-size: 12px;">USD STREAMED</div>
    </div>
    """, unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">Gian lận</div>
        <div class="metric-value" style="color: #FF4B4B;">{fraud_count:,} ({fraud_rate:.1f}%)</div>
        <div style="color: #FF4B4B; font-size: 12px;">CẢNH BÁO</div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# BIỂU ĐỒ VÀ BẢNG BẢN GHI THỜI GIAN THỰC
if fact_df.empty:
    st.warning("⚡ Data Lake Tầng Silver đang chờ giao dịch streaming mới từ Kafka...")
    st.info("Hãy đảm bảo container `kafka-producer` và `spark-streaming-consumer` đang chạy trong Docker.")
else:
    chart_col1, chart_col2 = st.columns(2)

    # 6.1 Biểu đồ đường Tốc độ giao dịch theo thời gian
    with chart_col1:
        st.subheader("📈 Tốc độ nạp giao dịch Real-Time")
        if "transaction_timestamp" in fact_df.columns:
            fact_df["ts_minute"] = pd.to_datetime(fact_df["transaction_timestamp"]).dt.strftime('%H:%M')
            timeline_df = fact_df.groupby("ts_minute").size().reset_index(name="transaction_count")
            
            fig_line = px.line(
                timeline_df.tail(20),
                x="ts_minute",
                y="transaction_count",
                markers=True,
                labels={"ts_minute": "Thời gian", "transaction_count": "Số giao dịch"},
                color_discrete_sequence=["#00D4FF"]
            )
            fig_line.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320
            )
            st.plotly_chart(fig_line, use_container_width=True)

    # 6.2 Biểu đồ tròn Cơ cấu hình thức quẹt thẻ
    with chart_col2:
        st.subheader("💳 Cơ cấu hình thức quẹt thẻ (Chip vs Swipe)")
        if "use_chip" in fact_df.columns:
            chip_counts = fact_df["use_chip"].value_counts().reset_index()
            chip_counts.columns = ["Hình thức", "Số lượng"]

            fig_donut = px.pie(
                chip_counts,
                values="Số lượng",
                names="Hình thức",
                hole=0.5,
                color_discrete_sequence=px.colors.sequential.Darkmint
            )
            fig_donut.update_layout(
                template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=320
            )
            st.plotly_chart(fig_donut, use_container_width=True)

    # 6.3 Bảng 15 giao dịch mới nhất (Tô màu đỏ cho gian lận)
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
        st.dataframe(styled_df, use_container_width=True, height=400)
    else:
        st.dataframe(recent_tx, use_container_width=True, height=400)

# TỰ ĐỘNG REFRESH DỮ LIỆU DÙNG STREAMLIT RERUN
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
