# ====================================================================================================
# start_airflow.ps1 - Khoi Dong Apache Airflow Orchestration & Kiem Tra DAG
# ====================================================================================================

param(
    [switch]$TestOnly,
    [switch]$Docker
)

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  [AIRFLOW] DIEU PHOI KHO DU LIEU BANG APACHE AIRFLOW ORCHESTRATION" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Kiem tra DAG integrity bang Python
Write-Host "[1/2] Kiem tra tinh toan ven cua DAG bang script kiem thu noi bo..." -ForegroundColor Cyan
python src/tools/test_airflow_dag.py
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERROR] DAG khong hop le!" -ForegroundColor Red
    exit 1
}

if ($TestOnly) {
    Write-Host "`n[OK] Che do -TestOnly: Kiem tra DAG thanh cong. Dung tai day." -ForegroundColor Green
    exit 0
}

# 2. Khoi dong Docker Compose cho Airflow (neu nguoi dung yeu cau hoac co Docker)
Write-Host ""
Write-Host "[2/2] Khoi dong Apache Airflow bang Docker Compose..." -ForegroundColor Cyan
try {
    $dockerRunning = docker ps 2>$null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  -> Dang khoi dong cac container: postgres-airflow, airflow-webserver, airflow-scheduler..." -ForegroundColor Yellow
        docker compose up -d postgres-airflow airflow-webserver airflow-scheduler
        Write-Host "`n  [SUCCESS] AIRFLOW DA DUOC KHOI DONG THANH CONG!" -ForegroundColor Green
        Write-Host "  -> Web UI Airflow   : http://localhost:8085" -ForegroundColor Yellow
        Write-Host "  -> Tai khoan dang nhap: admin / admin" -ForegroundColor Yellow
        Write-Host "  -> DAG ID           : credit_management_lakehouse_dwh" -ForegroundColor Yellow
    } else {
        Write-Host "  [NOTE] Docker Desktop chua bat hoac khong kha dung tren may nay." -ForegroundColor Yellow
        Write-Host "  DAG da duoc xac thuc cu phap va san sang de trien khai tren moi truong Docker/Production." -ForegroundColor Green
    }
} catch {
    Write-Host "  [NOTE] Khong the goi lenh docker. DAG da san sang tai thu muc dags/." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan

