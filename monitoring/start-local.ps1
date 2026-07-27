# SnapTrade local stack (Windows native — Docker engine unavailable)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $Root "docker-compose.yml"))) {
  $Root = "c:\Users\SSAFY\Downloads\ssafy_5_july\SnapTrade"
}

$PromDir = Join-Path $Root "monitoring\tools\prometheus-2.54.1.windows-amd64"
$PromConfig = Join-Path $Root "monitoring\prometheus\prometheus.local.yml"
$PromData = Join-Path $Root "monitoring\data\prometheus"
$MysqlBin = "C:\Program Files\MySQL\MySQL Server 8.4\bin\mysqld.exe"
$MysqlIni = Join-Path $Root "monitoring\data\my.ini"
$RedisExe = (Get-Command redis-server -ErrorAction SilentlyContinue).Source
if (-not $RedisExe) {
  $RedisExe = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\taizod1024.redis-windows-fork*" -Recurse -Filter "redis-server.exe" |
    Select-Object -First 1 -ExpandProperty FullName
}

New-Item -ItemType Directory -Force -Path $PromData | Out-Null

function Test-Port($port) {
  return [bool](Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue)
}

# MySQL
if (-not (Test-Port 3306)) {
  if (-not (Test-Path $MysqlBin)) { throw "mysqld.exe not found at $MysqlBin" }
  if (-not (Test-Path $MysqlIni)) { throw "MySQL config missing: $MysqlIni" }
  Start-Process -FilePath $MysqlBin -ArgumentList "--defaults-file=$MysqlIni" -WindowStyle Minimized
  Write-Host "Started MySQL on :3306"
} else {
  Write-Host "MySQL already listening on :3306"
}

# Redis
if (-not (Test-Port 6379)) {
  if (-not $RedisExe) { throw "redis-server.exe not found" }
  Start-Process -FilePath $RedisExe -ArgumentList "--appendonly","yes" -WindowStyle Minimized
  Write-Host "Started Redis on :6379"
} else {
  Write-Host "Redis already listening on :6379"
}

# Prometheus
if (-not (Test-Port 9090)) {
  Start-Process -FilePath (Join-Path $PromDir "prometheus.exe") `
    -ArgumentList "--config.file=$PromConfig","--storage.tsdb.path=$PromData","--web.listen-address=0.0.0.0:9090" `
    -WorkingDirectory $PromDir `
    -WindowStyle Minimized
  Write-Host "Started Prometheus on :9090"
} else {
  Write-Host "Prometheus already listening on :9090"
}

# Grafana (Windows service)
$grafana = Get-Service Grafana -ErrorAction SilentlyContinue
if ($grafana) {
  if ($grafana.Status -ne "Running") {
    Start-Service Grafana
    Write-Host "Started Grafana service"
  } else {
    Write-Host "Grafana already running"
  }
} else {
  Write-Host "Grafana service not found — open Grafana manually if needed"
}

Write-Host ""
Write-Host "Prometheus : http://localhost:9090"
Write-Host "Grafana    : http://localhost:3000  (default admin/admin)"
Write-Host "Metrics    : http://localhost:8080/api/actuator/prometheus  (app must be running)"
Write-Host "Redis      : localhost:6379"
Write-Host "MySQL      : localhost:3306  (root/root, db=snap_trade)"
