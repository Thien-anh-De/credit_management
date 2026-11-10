"""
Apache Airflow DAG: Credit Management Gold Layer DWH Orchestrator
==================================================================
Kiến trúc phân tầng:
  - Bronze & Silver: Tiếp nhận liên tục 24/7 bằng Apache Kafka & Spark Structured Streaming.
  - Gold DWH & Data Marts (DAG này phụ trách): Lập lịch định kỳ (Scheduled Batch ELT)
    để nạp từ Silver sạch vào Gold Data Warehouse chuẩn Kimball Star Schema, tính toán
    SCD Type 2 Point-in-Time Join, cuộn tháp Inmon và thực thi chốt kiểm toán chất lượng.
"""
import os
from datetime import datetime, timedelta

# Import an toàn tương thích nhiều phiên bản Airflow (2.x)
try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    try:
        from airflow.operators.empty import EmptyOperator
    except ImportError:
        from airflow.operators.dummy import DummyOperator as EmptyOperator
except ImportError:
    # Cho phép import và test syntax độc lập khi chưa có môi trường Airflow
    DAG = object
    BashOperator = object
    EmptyOperator = object


# ── Cấu hình mặc định cho các Task (Default Args) ──────────────────────────
default_args = {
    "owner": "DataEngineeringTeam",
    "depends_on_past": False,
    "email": ["data-alerts@bank.com.vn"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}

# ── Khởi tạo DAG ───────────────────────────────────────────────────────────
if DAG is not object:
    dag = DAG(
        dag_id="credit_management_gold_dwh",
        default_args=default_args,
        description="Scheduled Batch ELT from Silver Stream Lake into Kimball Star Schema Gold DWH",
        schedule_interval="0 2 * * *",  # Lập lịch chạy định kỳ hàng ngày lúc 02:00 AM (sau ca chốt sổ)
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["gold-dwh", "kimball", "inmon", "scd2", "fintech", "audit"],
        doc_md="""
        # 💳 Credit Card Gold DWH Orchestration DAG
        ## Kiến trúc tách biệt Stream & Batch
        * **Streaming Ingestion (Kafka + Spark Consumer):** Chạy liên tục 24/7 đưa sự kiện quẹt thẻ POS từ Bronze vào Silver.
        * **Gold DWH Orchestration (Airflow DAG này):** Định kỳ quét dữ liệu Silver sạch để:
          1. Xây dựng 4 bảng Dimension Gold song song (Date SCD0, Merchant SCD1, Users SCD2, Cards SCD2).
          2. Thực hiện **Point-in-Time Join** gắn chính xác phiên bản lịch sử vào `fact_transactions`.
          3. Tính toán tháp tổng hợp Inmon Roll-up (Ngày ➔ Tháng ➔ Quý) phục vụ BI Dashboard siêu tốc.
          4. Thực thi chốt kiểm định **Quality Gate & Audit Reconciliation** (0 orphan FK, bảo toàn dòng tiền 100%).
        """
    )

    # 1. Pipeline Start & End Markers
    start_gold_pipeline = EmptyOperator(
        task_id="start_gold_pipeline",
        dag=dag
    )

    end_gold_pipeline = EmptyOperator(
        task_id="end_gold_pipeline",
        dag=dag
    )

    # 2. Stage 1: Build Gold Dimensions in Parallel (Đọc từ Silver Stream)
    task_dim_date = BashOperator(
        task_id="build_dim_date_gold",
        bash_command="python src/dwh_jobs/01_build_dim_date.py",
        dag=dag
    )

    task_dim_merchant = BashOperator(
        task_id="build_dim_merchant_gold",
        bash_command="python src/dwh_jobs/02_build_dim_merchant.py",
        dag=dag
    )

    task_dim_users_scd2 = BashOperator(
        task_id="build_dim_users_scd2_gold",
        bash_command="python src/dwh_jobs/03_build_dim_users_scd2.py",
        dag=dag
    )

    task_dim_cards_scd2 = BashOperator(
        task_id="build_dim_cards_scd2_gold",
        bash_command="python src/dwh_jobs/04_build_dim_cards_scd2.py",
        dag=dag
    )

    # 3. Stage 2: Build Gold Fact with SCD2 Point-in-Time Join
    task_fact_transactions = BashOperator(
        task_id="build_fact_transactions_pit_join_gold",
        bash_command="python src/dwh_jobs/05_build_fact_transactions.py",
        dag=dag
    )

    # 4. Stage 3: Inmon Summary Rollup Layers (Data Marts)
    task_summary_layers = BashOperator(
        task_id="build_inmon_summary_layers",
        bash_command="python src/dwh_jobs/06_build_summary_layers.py",
        dag=dag
    )

    # 5. Stage 4: Quality Gate & Audit Reconciliation Suite
    task_dq_benchmark = BashOperator(
        task_id="data_quality_and_benchmark_gate",
        bash_command="python src/tests/07_dq_and_benchmark.py",
        dag=dag
    )

    # ── Thiết lập đồ thị phụ thuộc (Task Dependency Graph) ─────────────────
    # Bước 1: Kích hoạt đồng thời 4 task tạo Dimensions từ Silver
    start_gold_pipeline >> [task_dim_date, task_dim_merchant, task_dim_users_scd2, task_dim_cards_scd2]

    # Bước 2: Khi cả 4 Dimensions sẵn sàng -> Nạp Fact bằng Point-in-Time Join
    [task_dim_date, task_dim_merchant, task_dim_users_scd2, task_dim_cards_scd2] >> task_fact_transactions

    # Bước 3: Fact xong -> Xây dựng tháp tổng hợp Inmon Roll-up
    task_fact_transactions >> task_summary_layers

    # Bước 4: Summary xong -> Kích hoạt kiểm thử Quality Gate & Đối soát kiểm toán
    task_summary_layers >> task_dq_benchmark >> end_gold_pipeline
