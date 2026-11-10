$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    Write-Host "停止 OpenCompute AI 容器（保留 SQLite 和模型数据卷）..." -ForegroundColor Cyan
    docker compose --profile sim down
    if ($LASTEXITCODE -ne 0) { throw "停止失败，请确认 Docker Desktop 正在运行。" }
    Write-Host "已停止。数据卷已保留；下次启动不会自动清空历史数据。" -ForegroundColor Green
}
finally { Pop-Location }
