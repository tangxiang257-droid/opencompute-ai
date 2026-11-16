$ErrorActionPreference = "Continue"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    Write-Host "=== OpenCompute AI 容器状态 ===" -ForegroundColor Cyan
    docker compose ps
    Write-Host ""
    Write-Host "=== API 健康状态 ===" -ForegroundColor Cyan
    try { Invoke-RestMethod -Uri "http://127.0.0.1:8000/health" -TimeoutSec 4 | Format-List }
    catch { Write-Warning "API 暂不可访问：$($_.Exception.Message)" }
    Write-Host ""
    Write-Host "=== 最近 80 行 API 日志 ===" -ForegroundColor Cyan
    docker compose logs --tail 80 opencompute
    Write-Host ""
    Write-Host "如需查看 Worker 日志：docker compose --profile sim logs --tail 80 worker"
}
finally { Pop-Location }
