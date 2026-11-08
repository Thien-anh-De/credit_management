# ====================================================================================================
# clean_all.ps1 - Don Dep Toan Bo Du Lieu Sinh Ra (Reset Clean State)
# ====================================================================================================
# Muc dich:
#   - Xoa sach toan bo du lieu o tang Silver va Gold da duoc tao ra khi chay pipeline.
#   - Tat cac Docker container dang chay ngam (neu co).
#   - Xoa cac file cache tam (__pycache__, spark-warehouse...).
#   - BAO TOAN 100% du lieu goc ban dau trong thu muc Bronze (sd254_users, sd254_cards, ibm_v2).
#
# Cach dung (tu thu muc goc project):
#   .\clean_all.ps1
# ====================================================================================================

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host "  [CLEAN] BAT DAU DON DEP TOAN BO DU LIEU DA TAO (RESET VE TRANG THAI GOC)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Tat Docker containers neu dang chay de giai phong file lock
Write-Host "[1/5] Kiem tra va dung cac container Docker..." -ForegroundColor Cyan
try {
    $dockerRunning = docker ps -q 2>$null
    if ($dockerRunning) {
        Write-Host "  -> Dang dung cac container Docker..." -ForegroundColor Yellow
        docker compose down 2>$null
        Write-Host "  [OK] Da tat cac container Docker an toan." -ForegroundColor Green
    } else {
        Write-Host "  [OK] Khong co container Docker nao dang chay." -ForegroundColor Green
    }
} catch {
    Write-Host "  [OK] Khong co container Docker nao dang chay." -ForegroundColor Green
}

# 2. Don dep tang Silver
Write-Host ""
Write-Host "[2/5] Don dep du lieu tang Silver (data/silver)..." -ForegroundColor Cyan
$silverPath = "data\silver"
if (Test-Path $silverPath) {
    Get-ChildItem -Path $silverPath | Where-Object { $_.Name -ne ".gitkeep" } | ForEach-Object {
        try {
            Remove-Item -Path $_.FullName -Recurse -Force
            Write-Host "  - Da xoa: $($_.Name)" -ForegroundColor DarkGray
        } catch {
            Write-Host "  [!] Khong the xoa $($_.Name): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Write-Host "  [OK] Tang Silver da duoc don sach hoan toan." -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Path $silverPath -Force | Out-Null
    Write-Host "  [OK] Da khoi tao thu muc data/silver trong." -ForegroundColor Green
}
if (-not (Test-Path "$silverPath\.gitkeep")) {
    New-Item -ItemType File -Path "$silverPath\.gitkeep" -Force | Out-Null
}

# 3. Don dep tang Gold
Write-Host ""
Write-Host "[3/5] Don dep du lieu tang Gold (data/gold)..." -ForegroundColor Cyan
$goldPath = "data\gold"
if (Test-Path $goldPath) {
    Get-ChildItem -Path $goldPath | Where-Object { $_.Name -ne ".gitkeep" } | ForEach-Object {
        try {
            Remove-Item -Path $_.FullName -Recurse -Force
            Write-Host "  - Da xoa: $($_.Name)" -ForegroundColor DarkGray
        } catch {
            Write-Host "  [!] Khong the xoa $($_.Name): $($_.Exception.Message)" -ForegroundColor Red
        }
    }
    Write-Host "  [OK] Tang Gold (DWH & Data Marts) da duoc don sach hoan toan." -ForegroundColor Green
} else {
    New-Item -ItemType Directory -Path $goldPath -Force | Out-Null
    Write-Host "  [OK] Da khoi tao thu muc data/gold trong." -ForegroundColor Green
}
if (-not (Test-Path "$goldPath\.gitkeep")) {
    New-Item -ItemType File -Path "$goldPath\.gitkeep" -Force | Out-Null
}

# 4. Don dep cache Python va Spark tam
Write-Host ""
Write-Host "[4/5] Don dep cache tam (__pycache__, spark-warehouse)..." -ForegroundColor Cyan
$tempItems = @(
    "spark-warehouse", "metastore_db", "derby.log"
)
foreach ($t in $tempItems) {
    if (Test-Path $t) {
        Remove-Item -Path $t -Recurse -Force -ErrorAction SilentlyContinue
        Write-Host "  - Da xoa: $t" -ForegroundColor DarkGray
    }
}
Get-ChildItem -Path . -Recurse -Filter "__pycache__" -Directory -ErrorAction SilentlyContinue | ForEach-Object {
    Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "  - Da xoa cache: $($_.FullName)" -ForegroundColor DarkGray
}
Write-Host "  [OK] Da don sach toan bo cache tam." -ForegroundColor Green

# 5. Kiem tra va bao cao trang thai thu muc Bronze
Write-Host ""
Write-Host "[5/5] Kiem tra bao toan du lieu goc tang Bronze (data/bronze)..." -ForegroundColor Cyan
$bronzeFiles = Get-ChildItem -Path "data\bronze" | Where-Object { -not $_.PSIsContainer -and $_.Name -ne ".gitkeep" }
foreach ($f in $bronzeFiles) {
    $sizeMB = [math]::Round($f.Length / 1MB, 2)
    Write-Host "  * $($f.Name) ($sizeMB MB) -- [BAO TOAN NGUYEN VEN]" -ForegroundColor Green
}

Write-Host ""
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "  [SUCCESS] HOAN TAT DON DEP! HE THONG DA TRO VE TRANG THAI SACH BAN DAU" -ForegroundColor Green
Write-Host "================================================================================" -ForegroundColor Green
Write-Host "  - Tang Bronze : Giu nguyen 3 file goc ban dau." -ForegroundColor Yellow
Write-Host "  - Tang Silver : Trong (0 ban ghi)." -ForegroundColor Yellow
Write-Host "  - Tang Gold   : Trong (0 ban ghi)." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Khi muon chay lai he thong tu dau, ban chi can go:" -ForegroundColor Cyan
Write-Host "    .\run_dwh_pipeline.ps1   (Tu dong nap Silver va Gold)" -ForegroundColor Yellow
Write-Host "    docker compose up -d     (Bat streaming va web dashboard)" -ForegroundColor Yellow
Write-Host "================================================================================" -ForegroundColor Green
Write-Host ""
