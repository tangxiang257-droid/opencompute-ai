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
        Write-Host "Generated .env with two independent local secrets." -ForegroundColor Green
        return @{ Admin = $admin; Join = $join }
    }
    $existingLines = Get-Content -LiteralPath $envPath
    $admin = Read-DotEnvValue "OPENCOMPUTE_ADMIN_KEY" $existingLines
    $join = Read-DotEnvValue "OPENCOMPUTE_JOIN_KEY" $existingLines
    if ($admin.Length -lt 16 -or $join.Length -lt 16 -or
        $admin -match "replace-with|your-" -or
        $join -match "replace-with|your-" -or $admin -eq $join) {
        throw "Invalid OPENCOMPUTE_ADMIN_KEY / OPENCOMPUTE_JOIN_KEY in .env. Set two different random secrets of at least 16 characters, or back up and remove .env to let this launcher generate them."
    }
    return @{ Admin = $admin; Join = $join }
}

Push-Location $repoRoot
try {
    Write-Host ""
    Write-Host "=== OpenCompute AI Local Launcher ===" -ForegroundColor Cyan
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "Docker CLI not found. Install and start Docker Desktop: https://www.docker.com/products/docker-desktop/"
    }
    Write-Host "Checking Docker Engine..."
    & docker info --format "{{.ServerVersion}}" | Out-Host
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Engine is not running. Open Docker Desktop, wait until it shows Running, then retry."
    }
    $secrets = Get-OrCreateSecrets
    Write-Host "Validating Docker Compose configuration..."
    & docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw "docker compose config validation failed. Check .env and the Compose files." }

    if ($SimulateWorkers) {
        Write-Host "Building and starting the API plus 3 simulated CPU Workers..." -ForegroundColor Cyan
        & docker compose --profile sim up --build -d --scale worker=3
    }
    else {
        Write-Host "Building and starting the OpenCompute API..." -ForegroundColor Cyan
        & docker compose up --build -d
    }
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed. Run .\scripts\status-local.ps1 to inspect container status." }

    Write-Host "Waiting for the API to become healthy..."
    $ready = $false
    for ($i = 0; $i -lt 45; $i++) {
        try {
            $health = Invoke-RestMethod -Uri "$apiUrl/health" -TimeoutSec 3
            if ($health.status -eq "ok") { $ready = $true; break }
        }
        catch { Start-Sleep -Seconds 2 }
    }
    if (-not $ready) {
        Write-Warning "Containers started, but the API did not become healthy within 90 seconds. Run .\scripts\status-local.ps1 to inspect logs."
    }
    else { Write-Host "API is ready: $apiUrl" -ForegroundColor Green }

    if ($SimulateWorkers -and $ready) {
        Write-Host "Waiting for simulated Workers to register..."
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
        if ($onlineCount -ge 3) { Write-Host "Detected $onlineCount online Workers." -ForegroundColor Green }
        else { Write-Warning "Detected $onlineCount online Workers (target: 3). Run .\scripts\status-local.ps1 to inspect." }
    }

    Write-Host ""
    Write-Host "Website: $apiUrl/" -ForegroundColor Cyan
    $displayAdminKey = $secrets.Admin
    Write-Host ("Admin key (keep it private): " + $displayAdminKey) -ForegroundColor Yellow
    Write-Host "Paste the admin key into the dashboard header. It is stored in .env; never commit that file to GitHub."
    if (-not $NoBrowser -and $ready) { Start-Process $apiUrl }
}
catch {
    Write-Host ""
    $errorMessage = $_.Exception.Message
    Write-Host ("Startup failed: " + $errorMessage) -ForegroundColor Red
    exit 1
}
finally { Pop-Location }
