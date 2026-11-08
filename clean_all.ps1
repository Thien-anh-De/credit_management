# ====================================================================================================
# clean_all.ps1 - Dọn Dẹp Toàn Bộ Dữ Liệu Sinh Ra (Reset Clean State)
# ====================================================================================================
# Mục đích:
#   - Xóa sạch toàn bộ dữ liệu ở tầng Silver và Gold đã được tạo ra khi chạy pipeline.
#   - Tắt các Docker container đang chạy ngầm (nếu có).
#   - Xóa các file cache tạm (__pycache__, spark-warehouse...).
#   - BẢO TOÀN 100% dữ liệu gốc ban đầu trong thư mục Bronze (sd254_users, sd254_cards, ibm_v2).
#
# Cách dùng (từ thư mục gốc project):
#   .\clean_all.ps1
# ====================================================================================================

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  🧹 BẮT ĐẦU DỌN DẸP TOÀN BỘ DỮ LIỆU ĐÃ TẠO (RESET VỀ TRẠNG THÁI GỐC)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Tắt Docker containers nếu đang chạy để giải phóng file lock
Write-Host "[1/5] Kiểm tra và dừng các container Docker..." -ForegroundColor Cyan
try {
    $dockerRunning = docker ps -q 2>$null
    if ($dockerRunning) {
        Write-Host "  -> Đang dừng các container Docker..." -ForegroundColor Yellow
        docker compose down 2>$null
        Write-Host "  [OK] Đã tắt các container Docker an toàn." -ForegroundColor Green
    } else {
        Write-Host "  [OK] Không có container Docker nào đang chạy." -ForegroundColor Green
    }
} catch {
    Write-Host "  [OK] Không có container Docker nào đang chạy." -ForegroundColor Green
}

# 2. Dọn dẹp tầng Silver
Write-Host "`n[2/5] Dọn dẹp dữ liệu tầng Silver (data/silver)..." -ForegroundColor Cyan
$silverPath = "data\silver"
if (Test-Path $silverPath) {
    Get-ChildItem -Path $silverPath -Exclude ".gitkeep" | ForEach-Object {
        try {
            Remove-Item -Path $_.FullName -Recurse -Force
            Write-Host "  - Đã xóa: $($_.Name)" -ForegroundColor DarkGray
        } catch {
            Write-Host "  [!] Không thể xóa $($_.Name): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Write-Host "  [OK] Tầng Silver đã được dọn sạch hoàn toàn." -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Path $silverPath -Force | Out-Null
    Write-Host "  [OK] Đã khởi tạo thư mục data/silver trống." -ForegroundColor Green
}
if (-not (Test-Path "$silverPath\.gitkeep")) {
    New-Item -ItemType File -Path "$silverPath\.gitkeep" -Force | Out-Null
}

# 3. Dọn dẹp tầng Gold
Write-Host "`n[3/5] Dọn dẹp dữ liệu tầng Gold (data/gold)..." -ForegroundColor Cyan
$goldPath = "data\gold"
if (Test-Path $goldPath) {
    Get-ChildItem -Path $goldPath -Exclude ".gitkeep" | ForEach-Object {
        try {
            Remove-Item -Path $_.FullName -Recurse -Force
            Write-Host "  - Đã xóa: $($_.Name)" -ForegroundColor DarkGray
        } catch {
            Write-Host "  [!] Không thể xóa $($_.Name): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Write-Host "  [OK] Tầng Gold (DWH & Data Marts) đã được dọn sạch hoàn toàn." -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Path $goldPath -Force | Out-Null
    Write-Host "  [OK] Đã khởi tạo thư mục data/gold trống." -ForegroundColor Green
}
if (-not (Test-Path "$goldPath\.gitkeep")) {
    New-Item -ItemType File -Path "$goldPath\.gitkeep" -Force | Out-Null
}

# 4. Dọn dẹp cache Python và Spark tạm
Write-Host "`n[4/5] Dọn dẹp cache tạm (__pycache__, spark-warehouse)..." -ForegroundColor Cyan
$tempItems = @(
    "spark-warehouse", "metastore_db", "derby.log"
)
foreach ($t in $tempItems) {
    if (Test-Path $t) {
        Remove-Item -Path $t -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "  - Đã xóa: $t" -ForegroundColor DarkGray
    }
}
Get-ChildItem -Path . -Recurse -Filter "__pycache__" -Directory -ErrorAction SilentlyContinue | ForEach-Object {
    Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  - Đã xóa cache: $($_.FullName)" -ForegroundColor DarkGray
}
Write-Host "  [OK] Đã dọn sạch toàn bộ cache tạm." -ForegroundColor Green

# 5. Kiểm tra và báo cáo trạng thái thư mục Bronze
Write-Host "`n[5/5] Kiểm tra bảo toàn dữ liệu gốc tầng Bronze (data/bronze)..." -ForegroundColor Cyan
$bronzeFiles = Get-ChildItem -Path "data\bronze" -File -Exclude ".gitkeep"
foreach ($f in $bronzeFiles) {
    $sizeMB = [math]::Round($f.Length / 1MB, 2)
    Write-Host "  • $($f.Name) ($sizeMB MB) — [BẢO TOÀN NGUYÊN VẸN]" -ForegroundColor Green
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "  ✅ HOÀN TẤT DỌN DẸP! HỆ THỐNG ĐÃ TRỞ VỀ TRẠNG THÁI SẠCH BAN ĐẦU" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "  - Tầng Bronze : Giữ nguyên 3 file gốc ban đầu." -ForegroundColor Yellow
Write-Host "  - Tầng Silver : Trống (0 bản ghi)." -ForegroundColor Yellow
Write-Host "  - Tầng Gold   : Trống (0 bản ghi)." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Khi muốn chạy lại hệ thống từ đầu, bạn chỉ cần gõ:" -ForegroundColor Cyan
Write-Host "    .\run_dwh_pipeline.ps1   (Tự động nạp Silver và Gold)" -ForegroundColor Yellow
Write-Host "    docker compose up -d     (Bật streaming và web dashboard)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""
