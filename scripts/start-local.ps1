[CmdletBinding()]
param(
    [switch]$SimulateWorkers,
    [switch]$NoBrowser
)
$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $repoRoot ".env"
$apiUrl = "http://127.0.0.1:8000"

function New-UrlSafeSecret {
    $bytes = New-Object byte[] 32
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $rng.GetBytes($bytes)
        return [Convert]::ToBase64String($bytes).TrimEnd([char]61).Replace("+", "-").Replace("/", "_")
    }
    finally { $rng.Dispose() }
}
function Read-DotEnvValue([string]$Name, [string[]]$Lines) {
    $prefix = $Name + "="
    foreach ($line in $Lines) {
        $trimmed = $line.Trim()
        if ($trimmed.StartsWith($prefix)) {
            $value = $trimmed.Substring($prefix.Length).Trim()
            if ($value.Length -ge 2) {
                $first = [int][char]$value[0]
                $last = [int][char]$value[$value.Length - 1]
                if (($first -eq 34 -and $last -eq 34) -or
                    ($first -eq 39 -and $last -eq 39)) {
                    $value = $value.Substring(1, $value.Length - 2)
                }
            }
            return $value
        }
    }
    return ""
}
function Get-OrCreateSecrets {
    if (-not (Test-Path $envPath)) {
        $admin = New-UrlSafeSecret
        $join = New-UrlSafeSecret
        while ($join -eq $admin) { $join = New-UrlSafeSecret }
        $lines = @(
            "# Generated locally by scripts/start-local.ps1. Keep this file private.",
            "OPENCOMPUTE_ADMIN_KEY=$admin",
            "OPENCOMPUTE_JOIN_KEY=$join",
            "",
            "# Optional local model paths",
            "OPENCOMPUTE_TP_ROOT=./.opencompute/tp-sessions",
            "OPENCOMPUTE_TP_CHECKPOINTS=./.opencompute/checkpoints"
        )
        $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        [System.IO.File]::WriteAllLines($envPath, $lines, $utf8NoBom)
        Write-Host "已生成本机 .env 和两组独立密钥。" -ForegroundColor Green
        return @{ Admin = $admin; Join = $join }
    }
    $existingLines = Get-Content -LiteralPath $envPath
    $admin = Read-DotEnvValue "OPENCOMPUTE_ADMIN_KEY" $existingLines
    $join = Read-DotEnvValue "OPENCOMPUTE_JOIN_KEY" $existingLines
    if ($admin.Length -lt 16 -or $join.Length -lt 16 -or
        $admin -match "replace-with|替换|your-" -or
        $join -match "replace-with|替换|your-" -or $admin -eq $join) {
        throw ".env 中的 OPENCOMPUTE_ADMIN_KEY / OPENCOMPUTE_JOIN_KEY 无效。请编辑 .env 设置两组不同且至少 16 位的随机密钥；如果只是首次试运行且没有重要配置，也可以先备份后删除 .env，再重新运行本脚本自动生成。"
    }
    return @{ Admin = $admin; Join = $join }
}

Push-Location $repoRoot
try {
    Write-Host ""
    Write-Host "=== OpenCompute AI 本地启动器 ===" -ForegroundColor Cyan
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "没有找到 Docker CLI。请先安装并启动 Docker Desktop：https://www.docker.com/products/docker-desktop/"
    }
    Write-Host "检查 Docker Engine..."
    & docker info --format "{{.ServerVersion}}" | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Engine 尚未启动。请打开 Docker Desktop，等待状态变为 Running 后重试。"
    }
    $secrets = Get-OrCreateSecrets
    Write-Host "检查 Compose 配置..."
    & docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw "docker compose config 校验失败，请检查 .env 和 Compose 配置。" }

    if ($SimulateWorkers) {
        Write-Host "构建并启动 API + 3 个模拟 CPU Worker..." -ForegroundColor Cyan
        & docker compose --profile sim up --build -d --scale worker=3
    }
    else {
        Write-Host "构建并启动 OpenCompute API..." -ForegroundColor Cyan
        & docker compose up --build -d
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose 启动失败。可运行 .\scripts\status-local.ps1 查看容器状态。" }

    Write-Host "等待 API 就绪..."
    $ready = $false
    for ($i = 0; $i -lt 45; $i++) {
        try {
            $health = Invoke-RestMethod -Uri "$apiUrl/health" -TimeoutSec 3
            if ($health.status -eq "ok") { $ready = $true; break }
        }
        catch { Start-Sleep -Seconds 2 }
    }
    if (-not $ready) {
        Write-Warning "容器已启动，但 API 在 90 秒内尚未就绪。请运行 .\scripts\status-local.ps1 查看日志。"
    }
    else { Write-Host "API 已就绪：$apiUrl" -ForegroundColor Green }

    if ($SimulateWorkers -and $ready) {
        Write-Host "等待模拟 Worker 注册..."
        $onlineCount = 0
        for ($i = 0; $i -lt 30; $i++) {
            try {
                $cluster = Invoke-RestMethod -Uri "$apiUrl/v0.3/status" -Headers @{ "X-Admin-Key" = $secrets.Admin } -TimeoutSec 5
                $onlineCount = @($cluster.nodes | Where-Object { $_.online }).Count
                if ($onlineCount -ge 3) { break }
            }
            catch { }
            Start-Sleep -Seconds 2
        }
        if ($onlineCount -ge 3) { Write-Host "已检测到 $onlineCount 台在线 Worker。" -ForegroundColor Green }
        else { Write-Warning "当前检测到 $onlineCount 台在线 Worker（目标 3 台）。稍后可运行 .\scripts\status-local.ps1 检查。" }
    }

    Write-Host ""
    Write-Host "网站地址：$apiUrl/" -ForegroundColor Cyan
    Write-Host "管理员密钥（仅在本机使用，请勿分享）：$($secrets.Admin)" -ForegroundColor Yellow
    Write-Host "在网页顶部粘贴管理员密钥即可连接控制台。密钥保存在 .env 中，不要提交到 GitHub。"
    if (-not $NoBrowser -and $ready) { Start-Process $apiUrl }
}
catch {
    Write-Host ""
    Write-Host "启动未完成：$($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally { Pop-Location }

        $match = [regex]::Match($line, $pattern)
        if ($match.Success) {
            $value = $match.Groups[1].Value.Trim()
            if ($value.Length -ge 2 -and
                (($value.StartsWith('"') -and $value.EndsWith('"')) -or
                 ($value.StartsWith("'") -and $value.EndsWith("'")))) {
                $value = $value.Substring(1, $value.Length - 2)
            }
            return $value
        }
    }
    return ""
}
function Get-OrCreateSecrets {
    if (-not (Test-Path $envPath)) {
        $admin = New-UrlSafeSecret
        $join = New-UrlSafeSecret
        while ($join -eq $admin) { $join = New-UrlSafeSecret }
        $lines = @(
            "# Generated locally by scripts/start-local.ps1. Keep this file private.",
            "OPENCOMPUTE_ADMIN_KEY=$admin",
            "OPENCOMPUTE_JOIN_KEY=$join",
            "",
            "# Optional local model paths",
            "OPENCOMPUTE_TP_ROOT=./.opencompute/tp-sessions",
            "OPENCOMPUTE_TP_CHECKPOINTS=./.opencompute/checkpoints"
        )
        $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        [System.IO.File]::WriteAllLines($envPath, $lines, $utf8NoBom)
        Write-Host "已生成本机 .env 和两组独立密钥。" -ForegroundColor Green
        return @{ Admin = $admin; Join = $join }
    }
    $existingLines = Get-Content -LiteralPath $envPath
    $admin = Read-DotEnvValue "OPENCOMPUTE_ADMIN_KEY" $existingLines
    $join = Read-DotEnvValue "OPENCOMPUTE_JOIN_KEY" $existingLines
    if ($admin.Length -lt 16 -or $join.Length -lt 16 -or
        $admin -match "replace-with|替换|your-" -or
        $join -match "replace-with|替换|your-" -or $admin -eq $join) {
        throw ".env 中的 OPENCOMPUTE_ADMIN_KEY / OPENCOMPUTE_JOIN_KEY 无效。请编辑 .env 设置两组不同且至少 16 位的随机密钥；如果只是首次试运行且没有重要配置，也可以先备份后删除 .env，再重新运行本脚本自动生成。"
    }
    return @{ Admin = $admin; Join = $join }
}

Push-Location $repoRoot
try {
    Write-Host ""
    Write-Host "=== OpenCompute AI 本地启动器 ===" -ForegroundColor Cyan
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "没有找到 Docker CLI。请先安装并启动 Docker Desktop：https://www.docker.com/products/docker-desktop/"
    }
    Write-Host "检查 Docker Engine..."
    & docker info --format "{{.ServerVersion}}" | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Engine 尚未启动。请打开 Docker Desktop，等待状态变为 Running 后重试。"
    }
    $secrets = Get-OrCreateSecrets
    Write-Host "检查 Compose 配置..."
    & docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw "docker compose config 校验失败，请检查 .env 和 Compose 配置。" }

    if ($SimulateWorkers) {
        Write-Host "构建并启动 API + 3 个模拟 CPU Worker..." -ForegroundColor Cyan
        & docker compose --profile sim up --build -d --scale worker=3
    }
    else {
        Write-Host "构建并启动 OpenCompute API..." -ForegroundColor Cyan
        & docker compose up --build -d
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose 启动失败。可运行 .\scripts\status-local.ps1 查看容器状态。" }

    Write-Host "等待 API 就绪..."
    $ready = $false
    for ($i = 0; $i -lt 45; $i++) {
        try {
            $health = Invoke-RestMethod -Uri "$apiUrl/health" -TimeoutSec 3
            if ($health.status -eq "ok") { $ready = $true; break }
        }
        catch { Start-Sleep -Seconds 2 }
    }
    if (-not $ready) {
        Write-Warning "容器已启动，但 API 在 90 秒内尚未就绪。请运行 .\scripts\status-local.ps1 查看日志。"
    }
    else { Write-Host "API 已就绪：$apiUrl" -ForegroundColor Green }

    if ($SimulateWorkers -and $ready) {
        Write-Host "等待模拟 Worker 注册..."
        $onlineCount = 0
        for ($i = 0; $i -lt 30; $i++) {
            try {
                $cluster = Invoke-RestMethod -Uri "$apiUrl/v0.3/status" -Headers @{ "X-Admin-Key" = $secrets.Admin } -TimeoutSec 5
                $onlineCount = @($cluster.nodes | Where-Object { $_.online }).Count
                if ($onlineCount -ge 3) { break }
            }
            catch { }
            Start-Sleep -Seconds 2
        }
        if ($onlineCount -ge 3) { Write-Host "已检测到 $onlineCount 台在线 Worker。" -ForegroundColor Green }
        else { Write-Warning "当前检测到 $onlineCount 台在线 Worker（目标 3 台）。稍后可运行 .\scripts\status-local.ps1 检查。" }
    }

    Write-Host ""
    Write-Host "网站地址：$apiUrl/" -ForegroundColor Cyan
    Write-Host "管理员密钥（仅在本机使用，请勿分享）：$($secrets.Admin)" -ForegroundColor Yellow
    Write-Host "在网页顶部粘贴管理员密钥即可连接控制台。密钥保存在 .env 中，不要提交到 GitHub。"
    if (-not $NoBrowser -and $ready) { Start-Process $apiUrl }
}
catch {
    Write-Host ""
    Write-Host "启动未完成：$($_.Exception.Message)" -ForegroundColor Red
    exit 1
}
finally { Pop-Location }
