"""
ETL Audit & Quarantine Framework
================================
Cung cấp cơ chế Kiểm toán (Audit Logging) và Cách ly dữ liệu lỗi (Quarantine Zone):
1. ETL Audit Logger:
   - Ghi nhận nhật ký mỗi job: rows_read, rows_clean, rows_quarantined, status, duration.
   - Kiểm tra đẳng thức bảo toàn: rows_read == rows_clean + rows_quarantined (Zero Data Loss).
   - Lưu trữ tại: data/gold/etl_audit_log (Parquet) & data/gold/etl_audit_log.json (JSON/API).

2. Quarantine Zone (Dead Letter Queue):
   - Phân tách bản ghi lỗi (invalid/corrupted) ra khỏi luồng xử lý chính.
   - Gắn nhãn quarantine_reason, quarantined_at, quarantine_id.
   - Lưu trữ tại: data/quarantine/<dataset_name> (Parquet Snappy).
"""
import os
import sys
import json
import uuid
import shutil
from datetime import datetime
try:
    import pandas as pd
except ImportError:
    pd = None

# ── Base Directory ───────────────────────────────────────────────────────────
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
AUDIT_LOG_DIR = os.path.join(BASE_DIR, "data", "gold", "etl_audit_log")
AUDIT_LOG_PARQUET = os.path.join(AUDIT_LOG_DIR, "audit_log.parquet")
AUDIT_LOG_JSON = os.path.join(BASE_DIR, "data", "gold", "etl_audit_log.json")
QUARANTINE_BASE_DIR = os.path.join(BASE_DIR, "data", "quarantine")


class ETLAuditLogger:
    """Quản lý nhật ký kiểm toán ETL và đối soát dòng dữ liệu."""

    @staticmethod
    def log_job(
        spark=None,
        job_name: str = "",
        pipeline_stage: str = "",
        source_path: str = "",
        target_path: str = "",
        rows_read: int = 0,
        rows_clean: int = 0,
        rows_quarantined: int = 0,
        start_time: datetime = None,
        end_time: datetime = None,
        status: str = "SUCCESS",
        error_message: str = None,
        extra_metrics: dict = None
    ) -> dict:
        """Ghi nhận một bản ghi Audit Log vào Parquet và JSON."""
        start_time = start_time or datetime.now()
        end_time = end_time or datetime.now()
        duration_sec = round((end_time - start_time).total_seconds(), 2)
        is_balanced = (rows_read == (rows_clean + rows_quarantined))
        reconciliation_status = "100% BALANCED (ZERO LOSS)" if is_balanced else "MISMATCH (DATA LOSS DETECTED)"

        record_id = str(uuid.uuid4())[:8]
        audit_entry = {
            "audit_id": record_id,
            "job_name": job_name,
            "pipeline_stage": pipeline_stage,
            "source_path": os.path.relpath(source_path, BASE_DIR) if os.path.exists(source_path) else source_path,
            "target_path": os.path.relpath(target_path, BASE_DIR) if os.path.exists(target_path) else target_path,
            "rows_read": int(rows_read),
            "rows_clean": int(rows_clean),
            "rows_quarantined": int(rows_quarantined),
            "is_balanced": bool(is_balanced),
            "reconciliation_status": reconciliation_status,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": float(duration_sec),
            "status": status,
            "error_message": error_message or "None",
            "extra_metrics": json.dumps(extra_metrics or {})
        }

        # In thông báo trực quan ra console
        print("\n" + "=" * 70)
        print(f"📋 [ETL AUDIT LOG] {job_name} ({pipeline_stage})")
        print("-" * 70)
        print(f"  • Source File         : {audit_entry['source_path']}")
        print(f"  • Target Destination  : {audit_entry['target_path']}")
        print(f"  • Rows Read           : {rows_read:,}")
        print(f"  • Rows Clean Loaded   : {rows_clean:,}")
        print(f"  • Rows Quarantined    : {rows_quarantined:,}")
        print(f"  • Conservation Check  : {rows_read:,} == {rows_clean:,} + {rows_quarantined:,} [{reconciliation_status}]")
        print(f"  • Execution Duration  : {duration_sec}s | Status: {status}")
        print("=" * 70 + "\n")

        # 1. Ghi vào file JSON
        os.makedirs(os.path.dirname(AUDIT_LOG_JSON), exist_ok=True)
        history = []
        if os.path.exists(AUDIT_LOG_JSON):
            try:
                with open(AUDIT_LOG_JSON, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []
        
        # Lọc bỏ bản ghi cùng job_name đã có trước đó trong cùng phiên chạy để tránh trùng lặp
        history = [h for h in history if h.get("job_name") != job_name]
        history.append(audit_entry)
        with open(AUDIT_LOG_JSON, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)

        # 2. Ghi vào Parquet DWH Gold table bằng pandas/pyarrow (nhanh và an toàn)
        if pd is not None:
            try:
                os.makedirs(AUDIT_LOG_DIR, exist_ok=True)
                df_audit = pd.DataFrame(history)
                df_audit.to_parquet(AUDIT_LOG_PARQUET, index=False, engine="pyarrow")
            except Exception as e:
                print(f"[!] Warning: Khong the ghi Parquet audit log: {e}")

        return audit_entry


def route_to_quarantine(
    spark,
    df_raw,
    valid_condition,
    rejection_reason_col,
    dataset_name: str,
    output_silver_dir: str
):
    """
    Tách dữ liệu thành 2 luồng:
    1. df_clean: Dữ liệu sạch thỏa mãn valid_condition -> ghi vào Silver.
    2. df_quarantine: Dữ liệu vi phạm -> gắn cờ lý do và ghi vào data/quarantine/<dataset_name>.
    
    Bảo đảm đẳng thức: count(df_raw) == count(df_clean) + count(df_quarantine)
    """
    from pyspark.sql.functions import col, lit, md5, concat_ws

    quarantine_dir = os.path.join(QUARANTINE_BASE_DIR, dataset_name)
    os.makedirs(quarantine_dir, exist_ok=True)
    os.makedirs(output_silver_dir, exist_ok=True)

    # Cache df_raw để tối ưu tốc độ phân tách
    df_raw.cache()
    rows_read = df_raw.count()

    # Phân tách
    df_clean = df_raw.filter(valid_condition)
    df_quarantine = df_raw.filter(~valid_condition)

    rows_clean = df_clean.count()
    rows_quarantine = df_quarantine.count()

    # Nếu có bản ghi cách ly, gắn metadata kiểm toán
    if rows_quarantine > 0:
        df_quarantine_enriched = df_quarantine \
            .withColumn("quarantine_reason", rejection_reason_col) \
            .withColumn("quarantined_at", lit(datetime.now().isoformat())) \
            .withColumn("quarantine_id", md5(concat_ws("_", *[col(c) for c in df_quarantine.columns[:4]])))

        print(f"⚠️  [QUARANTINE] Phát hiện {rows_quarantine:,} bản ghi vi phạm quy tắc dữ liệu!")
        print(f"    -> Đang lưu trữ vào vùng cách ly: {quarantine_dir}")
        df_quarantine_enriched.write.mode("overwrite").parquet(quarantine_dir)
    else:
        # Nếu không có dòng lỗi nào, xóa thư mục cũ nếu có và ghi file rỗng
        if os.path.exists(quarantine_dir):
            shutil.rmtree(quarantine_dir, ignore_errors=True)
        os.makedirs(quarantine_dir, exist_ok=True)
        with open(os.path.join(quarantine_dir, ".empty_clean"), "w", encoding="utf-8") as f:
            f.write("0 quarantine records. 100% data passed quality gates.\n")

    return df_clean, df_quarantine, rows_read, rows_clean, rows_quarantine
