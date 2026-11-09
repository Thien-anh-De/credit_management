# ============================================================
# start_react_dashboard.ps1 - Khoi dong React.js Dashboard & FastAPI
# ============================================================

# Tu dong giai phong port 8000 neu co tien trinh cu dang chiem giu
$conn = Get-NetTCPConnection -LocalPort 8000 -ErrorAction SilentlyContinue
if ($conn) {
    $pids = $conn | Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($p in $pids) {
        if ($p -gt 0) {
            Write-Host "[INFO] Dang giai phong port 8000 (PID: $p)..." -ForegroundColor Yellow
            Stop-Process -Id $p -Force -ErrorAction SilentlyContinue
        }
    }
    Start-Sleep -Seconds 1
}

Write-Host "[INFO] Dang khoi dong Backend API Server (FastAPI - Port 8000)..." -ForegroundColor Cyan
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python src/api_server.py"

Start-Sleep -Seconds 2

Write-Host "[INFO] Dang khoi dong Frontend React.js (Vite)..." -ForegroundColor Green
Set-Location frontend
npm run dev -- --open
